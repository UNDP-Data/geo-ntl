import asyncio
import math
import os
from pathlib import Path
from typing import List, Tuple, Iterable
import logging
from rich.progress import Progress
import re
import httpx
from urllib.parse import urljoin

NRT = 'NRT'
STD = 'STD'
SOURCES = {
    NRT: 'https://nrt3.modaps.eosdis.nasa.gov/archive/allData/5200', #NRT lance
    STD: 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200' #LADDS
}
CONTENT_API = {
    NRT: 'https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/details/allData/5200',
    STD: 'https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details/allData/5200'
}
NTL_FILENAME_PATTERN = re.compile(
    r"^(?P<product>V[A-Z0-9_]+)\."
    r"A(?P<year>\d{4})(?P<doy>\d{3})\."
    r"(?P<tile>h(?P<h>\d{2})v(?P<v>\d{2}))\."
    r"(?P<version>\d{3})"
    r"(?:\.(?P<production_time>\d{13}))?"  # The optional timestamp
    r"\.h5$"
)

logger = logging.getLogger(__name__)

def get_intersecting_tiles(bbox: Tuple[float, float, float, float]) -> List[Tuple[int, int]]:
    """
    Identifies VIIRS Sinusoidal tiles (h, v) intersecting a geographic bounding box.
    bbox format: (min_lon, min_lat, max_lon, max_lat)
    """
    min_lon, min_lat, max_lon, max_lat = bbox

    # VIIRS standard sinusoidal grid is approx 10x10 degrees at the equator
    # h runs 0 to 35 (180W to 180E)
    # v runs 0 to 17 (90N to 90S)
    h_min = math.floor((min_lon + 180) / 10)
    h_max = math.floor((max_lon + 180) / 10)
    v_min = math.floor((90 - max_lat) / 10)
    v_max = math.floor((90 - min_lat) / 10)

    tiles = []
    for v in range(max(0, v_min), min(18, v_max + 1)):
        for h in range(max(0, h_min), min(36, h_max + 1)):
            tiles.append((h, v))

    return tiles


async def download_tile(
        client: httpx.AsyncClient,
        url: str,
        dest_path: Path,
        semaphore: asyncio.Semaphore,
        progress: Progress,
        task_id: int,
        max_retries: int = 3
) -> bool:
    """
    Asynchronously downloads a single tile with exponential backoff retries.
    Uses a semaphore to prevent overwhelming the LANCE servers.
    """
    async with semaphore:
        for attempt in range(max_retries):
            try:
                # Start the streaming request
                async with client.stream("GET", url) as response:
                    response.raise_for_status()

                    # Initialize progress bar for this specific file
                    total_bytes = int(response.headers.get("Content-Length", 0))
                    progress.update(task_id, total=total_bytes, visible=True, description=f"Downloading {dest_path.name}")
                    progress.start_task(task_id)
                    with open(dest_path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)
                            progress.advance(task_id, advance=len(chunk))

                progress.update(task_id, description=f"[green]✓ {dest_path.name}")

                return True

            except httpx.HTTPError as e:
                if attempt == max_retries - 1:
                    progress.update(task_id, description=f"[red]✗ {dest_path.name} (Failed)[/red]")
                    progress.console.print(f"[red]Error downloading {url}: {e}[/red]")
                    return False

                # Exponential backoff before retry (1s, 2s, 4s...)
                await asyncio.sleep(2 ** attempt)


async def fetch_alert_tiles(
        bbox: Tuple[float, float, float, float],
        year: int,
        doy: int,  # Day of Year (e.g., 83)
        client: httpx.AsyncClient,
        dest_dir: str,
        max_concurrency: int = 5
):
    """
    Main orchestrator for identifying and concurrently downloading NRT tiles.
    """
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    tiles = get_intersecting_tiles(bbox)
    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = []
    with Progress() as progress:
        for h, v in tiles:
            # Construct the predictive LANCE NRT3 URL
            filename = f"VNP46A1_NRT.A{year}{doy:03d}.h{h:02d}v{v:02d}.002.h5"
            url = f"https://nrt3.modaps.eosdis.nasa.gov/archive/allData/5200/VNP46A1_NRT/{year}/{doy:03d}/{filename}"



            filepath = dest_path / filename

            # Add an invisible task to the progress UI (becomes visible upon download start)
            task_id = progress.add_task(f"Waiting h{h:02d}v{v:02d}...", start=False, visible=False)

            # Create the asyncio task
            tasks.append(download_tile(client, url, filepath, semaphore, progress, task_id))

        # Execute all downloads concurrently
        results = await asyncio.gather(*tasks)

    success_count = sum(results)
    logger.info(f"[ RAPIDA Engine ] Successfully ingested {success_count}/{len(tiles)} tiles.")


from datetime import datetime, time, timedelta, date


def resolve_ntl_source(target_date: date =None) -> dict:
    """
    Routes a target night to the best available NASA product
    based on the RAPIDA latency-to-feature priority table.

    ### NASA VIIRS (VNP46) Feature Matrix
    | Feature | VNP46A1_NRT | VNP46A1 (Std) | VNP46A2_NRT | VNP46A2 (Std) |
    | :--- | :--- | :--- | :--- | :--- |
    | **Product ID** | VNP46A1_NRT | VNP46A1 | VNP46A2_NRT | VNP46A2 |
    | **Radiometry** | At-Sensor (TOA) | At-Sensor (TOA) | Surface Rad. | Surface Rad. |
    | **Latency** | < 3 Hours | 24–48 Hours | 12–24 Hours | 2–4 Days |
    | **Orbit** | Predictive | Definitive | Predictive | Definitive |
    | **Ancillary** | Forecast | Recorded | Forecast | MERRA-2 |
    | **Corrections**| Basic Geom. | Full Geom. | Prelim Lunar | Full Black Marble |

    Args:
        target_date (date): The calendar date of the nighttime observation.
    """
    now = datetime.now()

    # Anchor: 01:30 AM overpass of the target date
    overpass_dt = datetime.combine(target_date, time(1, 30))
    hours_since_overpass = (now - overpass_dt).total_seconds() / 3600

    logger.info(f'request at {now} - observation day {target_date} -  hours since overpass {hours_since_overpass}')

    if hours_since_overpass < 0:
        raise Exception(f"No imagery is available because overpass for {target_date} hasn't happened yet.")

    # --- Routing Logic (Strictly following the 3, 12, 24, 48 hour thresholds) ---

    if hours_since_overpass < 3:
        # TIER: EARLY FLASH
        # Target obtained as soon as the satellite clears the horizon + processing
        product, source = 'VNP46A1_NRT', 'NRT'

    elif hours_since_overpass < 24:
        # TIER: REFINED
        # Surface Radiance becomes available (Preliminary Lunar/Atmos corrections)
        product, source = 'VNP46A2_NRT', 'NRT'

    elif hours_since_overpass < 48:
        # TIER: VALIDATED
        # Definitive Orbit data arrives in the Archive (Full Geometric correction)
        product, source =  'VNP46A1', 'STD'

    else:
        # TIER: FINAL
        # The "Gold Standard" Black Marble product (Definitive + MERRA-2)
        product, source = 'VNP46A2', 'STD'

    return product, source

def interpolate_url(target_date:date=None, product:str=None, source_root_url:str=None, hseg:int=None, vseg:int=None):

    doy = int(target_date.strftime('%j'))
    filename = "{product}.A{year}{doy:03d}.h{hseg:02d}v{vseg:02d}.002.h5".format(
        product=product,year=target_date.year, doy=doy, hseg=hseg, vseg=vseg
    )
    return f"{source_root_url}/{product}/{target_date.year}/{doy:03d}/{filename}"





async def resolve_fresh_archive_file(product: str, target_date: date, tile: str, client: httpx.AsyncClient):
    """
    Directly scrapes the LADSWEB directory to find files too
    new for the CMR Search API.
    """
    year = target_date.year
    doy = target_date.strftime('%j')  # '092' for April 2

    # Path: /archive/allData/5200/VNP46A2/2026/092/
    dir_url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/{product}/{year}/{doy}/"

    logger.info(f"Searching directory directly: {dir_url}")
    response = await client.get(dir_url)

    if response.status_code == 200:
        # Match pattern: VNP46A2.A2026092.h19v05.002.ANY_TIMESTAMP.h5
        pattern = rf'({product}\.A{year}{doy}\.{tile}\.002\.\d{{13}}\.h5)'
        matches = re.findall(pattern, response.text)

        if matches:
            # Sort to get the latest production timestamp (e.g., 2026100...)
            latest_filename = sorted(list(set(matches)))[-1]
            return urljoin(dir_url, latest_filename)

    return None



async def urls_from_api(product:str=None, client:httpx.AsyncClient=None, content_url:str=None, tiles:Iterable=None):

    for hseg, vseg in tiles:
        tile = f'h{hseg:02d}v{vseg:02d}'
        # 1. Fetch the JSON directory listing
        response = await client.get(content_url)
        response.raise_for_status()

        # 2. Extract the file list (usually under the 'content' key)
        content = response.json()

        # # 3. Filter for the .h5 file matching your tile
        # # Example filename: VNP46A2.A2026092.h19v05.002.2026100104045.h5
        # matches = [
        #     f for f in files
        #     if f['name'].endswith('.h5') and f'.{tile}.' in f['name']
        # ]
        #
        # if not matches:
        #     return None
        #
        # # 4. Sort by name to get the latest production timestamp
        # latest_file = sorted(matches, key=lambda x: x['name'])[-1]
        #
        # # Return the absolute download URL
        # # The API-V2 usually provides 'path' or 'size'
        # # We can construct the download URL using the original root
        # return latest_file['name']

# async def list_ntl_by_bbox(product: str =None, target_date: date = None, bbox: tuple[float] = None):
#     """
#     Searches NASA CMR for all tiles intersecting a bounding box.
#
#     Args:
#         product (str): e.g., 'VNP46A2' or 'VNP46A1_NRT'
#         target_date (str): 'YYYY-MM-DD'
#         bbox (list): [min_lon, min_lat, max_lon, max_lat] (W, S, E, N)
#     """
#     base_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
#
#     # Convert list to CMR-friendly string: "W,S,E,N"
#     bbox_str = ",".join(map(str, bbox))
#
#     params = {
#         "short_name": product,
#         "temporal": f"{target_date}T00:00:00Z,{target_date}T23:59:59Z",
#         "bounding_box": bbox_str,
#         "page_size": 100  # Large enough to catch multiple tiles if necessary
#     }
#
#     async with httpx.AsyncClient() as client:
#         response = await client.get(base_url, params=params)
#
#         if response.status_code != 200:
#             return []
#
#         data = response.json()
#         granules = data.get('feed', {}).get('entry', [])
#
#         results = []
#         for g in granules:
#             # Filter links to find the actual .h5 data file
#             urls = g.get('links', [])
#             h5_url = next(
#                 (l['href'] for l in urls if l['href'].endswith('.h5') and 'data' in l.get('rel', '')),
#                 None
#             )
#
#             if h5_url:
#                 results.append({
#                     "filename": g.get('producer_granule_id'),
#                     "url": h5_url,
#                     "tile": re.search(r"h\d+v\d+", g.get('producer_granule_id')).group()
#                 })
#
#         # Remove duplicates if NASA has multiple versions; keep only the latest revision
#         # (This is the Feng Shui way to ensure you don't download the same tile twice)
#         unique_results = {res['tile']: res for res in sorted(results, key=lambda x: x['filename'])}.values()
#
#         return list(unique_results)


# ---------------------------------------------------------
# Example Execution Context (How the main app calls this)
# ---------------------------------------------------------
async def fetch_ntl_data(observation_date:date = None, bbox:Tuple[float] = None,
                         max_concurrency=6, dst_dir:str|Path = None):




    APP_KEY = os.getenv("EARTH_ACCESS_TOKEN")

    # Define custom timeout and connection pooling
    timeout = httpx.Timeout(10.0, read=30.0)
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)

    # Initialize the client ONCE, injecting the AppKey into the global headers
    headers = {"Authorization": f"Bearer {APP_KEY}"}
    dest_path = Path(dst_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    tiles = get_intersecting_tiles(bbox)

    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = []
    async with httpx.AsyncClient(headers=headers, timeout=timeout, limits=limits) as download_client:
        product, source = resolve_ntl_source(target_date=observation_date)
        root_source_content_url = CONTENT_API[source]
        #source_content_url = f'{root_source_content_url}/{product}/{observation_date.year}/{observation_date.strftime(format="%j"):}'
        logger.info(f'Going to fetch NTL data for {product}  from {source}')
        with Progress() as progress:
            for hseg, vseg in tiles:
                if source == NRT:
                    file_url = interpolate_url(
                        target_date=observation_date,product=product, source_root_url=root_source_content_url,
                        hseg=hseg, vseg=vseg
                    )
                print(file_url)
                #path = urlparse(file_url).path
        #
        #         # 2. Get the basename from that path
        #         file_name = os.path.basename(path)
        #
        #         filepath = dest_path / file_name
        #
        #         # Add an invisible task to the progress UI (becomes visible upon download start)
        #         task_id = progress.add_task(f"Waiting h{hseg:02d}v{vseg:02d}...", start=False, visible=False)
        #
        #         # Create the asyncio task
        #         tasks.append(download_tile(download_client, file_url, filepath, semaphore, progress, task_id))
        #
        #         # Execute all downloads concurrently
        #     results = await asyncio.gather(*tasks)
        #
        #     success_count = sum(results)
        #     logger.info(f"[ RAPIDA Engine ] Successfully ingested {success_count}/{len(tiles)} tiles.")




if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()
    import asyncio
    logging.basicConfig()
    logger = logging.getLogger('ntl')
    logger.setLevel(logging.INFO)
    target_date = '2026-04-12'

    target_bbox = (14.0, 48.5, 19.0, 51.0)  # Example Bounding Box
    target_date = date.fromisoformat(target_date)

    asyncio.run(fetch_ntl_data(observation_date=target_date, bbox=target_bbox, dst_dir='/data/NTL'))
