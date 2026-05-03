import os
import re
from pathlib import Path
from datetime import datetime
import h5py
from user_agent import generate_user_agent
import httpx
import logging
import math
from urllib.parse import urlparse
import asyncio
from rich.progress import Progress
logger = logging.getLogger()

agent = generate_user_agent(os='linux', device_type='desktop')
# Satellites and their product prefixes

SNPP = 'SNPP'
N20 = 'N20'
N21 = 'N21'


SATELLITES = {
    SNPP: 'VNP',
    N20: 'VJ1',
    N21: 'VJ2'
}
NRT = 'NRT'
STD = 'STD'

PRODUCTS = {
    NRT: ('46A1_NRT', '46A2_NRT'),
    STD: ('46A1', '46A2', '46A3', '46A4')
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
    r"(?:\.(?P<production_time>\d{13}))?"
    r"\.h5$"
)



def list_available_products():
    for sat in SATELLITES:
        for stream, products in PRODUCTS.items():
            for product in products:
                api_url = resolve_ntl_api(sat, product)
                if api_url is not None:
                    print(sat, product, api_url)


def get_intersecting_tiles(bbox: tuple[float, float, float, float]) -> list[tuple[int, int]]:
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

def resolve_ntl_api(sat:str=None, product:str=None):

    #based on logic from 2026
    if sat.upper() == N20 and product == '46A2_NRT':return
    if sat.upper() == N21 and product.endswith('_NRT'):return

    stream = NRT if product.endswith("_NRT") else STD
    product_code = f"{SATELLITES[sat.upper()]}{product}"
    return f"{CONTENT_API[stream]}/{product_code}/"


async def fetch_product_years(client:httpx.AsyncClient, api_url:str):
    try:
        resp = await client.get(api_url)
        resp.raise_for_status()
        data = resp.json()['content']
        return tuple(map(int, [itm['name'] for itm in data]))
    except Exception as e:
        logger.error(f'Failed on url  {api_url} with {e}')
        return []

async def fetch_product_doys(client:httpx.AsyncClient, api_url:str, year:int):
    try:
        year_api_url = os.path.join(api_url, f'{year}')
        resp  = await client.get(year_api_url)
        resp.raise_for_status()
        data = resp.json()['content']
        return tuple(map(int, [itm['name'] for itm in data]))
    except Exception as e:
        logger.error(f'Failed on url  {api_url} with {e}')
        return []


async def get_content_api_url(client:httpx.AsyncClient, sat: str, product: str, year: int, doy: int) -> str:
    """Builds the JSON API endpoint dynamically based on the product suffix."""
    target_datetime = datetime.strptime(f'{year}{doy:03d}', '%Y%j')
    root_api_url = resolve_ntl_api(sat=sat, product=product)
    if not root_api_url:
        raise Exception(f'No imagery exists for satellite  {sat} and product {product}')
    available_product_years = await fetch_product_years(client, root_api_url)
    if not year in available_product_years:
        raise Exception(f'No imagery exists for year {year} satellite {sat} and product {product}')
    available_year_doys = await fetch_product_doys(client, api_url=root_api_url, year=year)
    if not 'A3' in product:
        if not doy in available_year_doys:
            raise Exception(f'No imagery exists for {target_datetime:%Y-%m-%d} satellite {sat} product {product}')
        api_url = os.path.join(root_api_url, f'{year}', f'{doy:03d}')
    else:
        available_months = tuple(map(int, [datetime.strptime(f'{year}{e:03d}', '%Y%j').month for e in available_year_doys]))
        delta = [(i, abs(m-target_datetime.month)) for i, m in enumerate(available_months)]
        best_match = min(delta, key=lambda x: x[1])

        # Extract the index from the best match
        best_index = best_match[0]

        # Use the index to get the actual month
        closest_doy = available_year_doys[best_index]
        api_url = os.path.join(root_api_url, f'{year}', f'{closest_doy:03d}')


    # 3. Route to the correct API base URL
    return api_url

def create_vrt_from_local(h5_path: str, vrt_path: str = None):
    """
    Reads VIIRS HDF5 metadata natively and generates a QGIS-ready VRT XML file.
    No GDAL installation required.
    """
    if vrt_path is None:
        vrt_path = h5_path.replace(".h5", ".vrt")

    # 1. Extract the geographic bounds directly from the HDF5 attributes
    with h5py.File(h5_path, 'r') as f:

        def extract_val(attr_name):
            val = f.attrs[attr_name]
            # Try to grab the first element if it's an array/list, otherwise return the scalar
            try:
                return float(val[0])
            except (IndexError, TypeError):
                return float(val)

        west = extract_val('WestBoundingCoord')
        north = extract_val('NorthBoundingCoord')
        east = extract_val('EastBoundingCoord')
        south = extract_val('SouthBoundingCoord')

    # 2. Calculate the GDAL Geotransform
    # Daily VIIRS 46A1 tiles are always exactly 2400 x 2400
    width = 2400
    height = 2400

    pixel_width = (east - west) / width
    pixel_height = (south - north) / height  # This will be negative

    # 3. Format the GDAL connection string (Absolute path)
    abs_h5 = os.path.abspath(h5_path)
    subdataset = f'HDF5:"{abs_h5}"://HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data_Fields/DNB_At_Sensor_Radiance'

    # 4. Construct the XML string
    vrt_xml = f"""<VRTDataset rasterXSize="{width}" rasterYSize="{height}">
  <SRS dataAxisToSRSAxisMapping="2,1">EPSG:4326</SRS>
  <GeoTransform>{west}, {pixel_width}, 0.0, {north}, 0.0, {pixel_height}</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1">
    <NoDataValue>-999.90002</NoDataValue>
    <ColorInterp>Gray</ColorInterp>
    <SimpleSource>
      <SourceFilename relativeToVRT="0">{subdataset}</SourceFilename>
      <SourceBand>1</SourceBand>
      <SrcRect xOff="0" yOff="0" xSize="{width}" ySize="{height}" />
      <DstRect xOff="0" yOff="0" xSize="{width}" ySize="{height}" />
    </SimpleSource>
  </VRTRasterBand>
</VRTDataset>"""

    # 5. Save it next to the H5 file
    with open(vrt_path, "w") as f:
        f.write(vrt_xml)

    print(f"[+] Wrote QGIS VRT: {vrt_path}")
    return vrt_path
async def download_tile(
        client: httpx.AsyncClient,
        url: str,
        dest_path: Path,
        semaphore: asyncio.Semaphore,
        progress: Progress,
        task_id: int,
        max_retries: int = 3
) -> Path:
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
                    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
                    with open(tmp_path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)
                            progress.advance(task_id, advance=len(chunk))
                    tmp_path.rename(dest_path)
                progress.update(task_id, description=f"[green]✓ {dest_path.name}")

                return dest_path

            except httpx.HTTPError as e:
                if attempt == max_retries - 1:
                    progress.update(task_id, description=f"[red]✗ {dest_path.name} (Failed)[/red]")
                    progress.console.print(f"[red]Error downloading {url}: {e}[/red]")
                    return dest_path

                # Exponential backoff before retry (1s, 2s, 4s...)
                await asyncio.sleep(2 ** attempt)

async def discover_granules(client: httpx.AsyncClient, sat_key: str, prod_type: str,
                            year: int, doy: int):
    """
    Queries the MODAPS Content API and parses the native download links.
    """
    url = await get_content_api_url(client, sat_key, prod_type, year, doy)

    try:
        resp = await client.get(url)

        # 404 just means the data isn't processed/uploaded yet
        if resp.status_code == 404:
            return []

        resp.raise_for_status()
        valid_granules = []
        items = resp.json().get('content', [])
        for item in items:

            name = item['name']

            match = NTL_FILENAME_PATTERN.match(name)

            if match:
                meta = match.groupdict()

                # Extract the native link directly from the JSON payload
                download_link = item.get('downloadsLink') or item.get('fileURL') or item.get('url')

                valid_granules.append({
                    "satellite": sat_key,
                    "product": meta['product'],
                    "filename": name,
                    "size": item.get('size'),
                    "mtime": item.get('mtime'),
                    "tile": meta['tile'],
                    "url": download_link,
                    "meta": meta
                })

        return valid_granules

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP Error discovering {sat_key} {prod_type} : {e.response.status_code}")
        return []
    except Exception as e:
        logger.error(f"Error discovering {sat_key} {prod_type} : {e}")
        return []

async def locate_ntl_by_timestamp(
        client: httpx.AsyncClient,
        timestamp_str: str,
        sat_key: str,
        tile: str,
        product: str = "46A2_NRT",

):
    """
    Translates a CLI timestamp (YYYYMMDDHHMM) into a native NASA URL.
    """
    # 1. Parse the CLI timestamp
    dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M")
    year = dt.year
    doy = dt.timetuple().tm_yday

    logger.info(f"Target: {sat_key} | Date: {dt.strftime('%Y-%m-%d')} (DOY: {doy:03d}) | Tile: {tile}")

    # 2. Hit the JSON API using the engine we built
    granules = await discover_granules(client, sat_key, product, year, doy)


    # 3. Isolate the specific geographic tile
    tile_matches = [g for g in granules if g['meta']['tile'] == tile]

    if not tile_matches:
        logger.error(f"[-] No data found for tile {tile} on {year}{doy:03d}")
        return None

    # 4. Robustness: If multiple versions exist, grab the newest processing
    tile_matches.sort(
        key=lambda x: x['meta'].get('production_time') or '0',
        reverse=True
    )

    best_match = tile_matches[0]
    logger.info(f"[+] Found File: {best_match['filename']}")

    return best_match



async def fetch_winner(timestamp:str=None, satellite:str=None, product:str=None, bbox:tuple[float] = None, dst_dir='/tmp'):
    # Your App Key for MODAPS
    ea_token = os.environ.get('EARTHDATA_TOKEN')
    headers = {"Authorization": f"Bearer {ea_token}"}

    # The parameters from your CLI output

    tiles = get_intersecting_tiles(bbox=bbox)
    dest_path = Path(dst_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    tasks = []
    semaphore = asyncio.Semaphore(5)

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:

        with Progress(disable=False,  transient=False) as progress:
            for hseg, vseg in tiles:
                tile = f'h{hseg:02d}v{vseg:02d}'
                tasks.append(locate_ntl_by_timestamp(
                    client=client,
                    timestamp_str=timestamp,
                    sat_key=satellite,
                    tile=tile,
                    product=product
                ))
            results = await asyncio.gather(*tasks)


            if results:
                tasks = []
                for result in results:
                    url, size = result['url'], result['size']
                    print("\n--- Payload for obstore ---")
                    print(f"Direct URL: {url}")
                    print(f"Size: {size / (1024 * 1024):.2f} MB")
                    path = urlparse(url).path

                    # 2. Get the basename from that path
                    file_name = os.path.basename(path)

                    filepath = dest_path / file_name

                    # Add an invisible task to the progress UI (becomes visible upon download start)
                    task_id = progress.add_task(f"Waiting h{hseg:02d}v{vseg:02d}...",)

                    # Create the asyncio task
                    tasks.append(download_tile(client, url, filepath, semaphore, progress, task_id))

            # Execute all downloads concurrently
            res = await asyncio.gather(*tasks)

            #for local_file in res:
                # if local_file.exists():
                #     vrt = create_vrt_from_local(str(local_file))
            return res[0]







if __name__ == '__main__':
    import asyncio
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig()
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logger.name = 'ntlcli'

    target_date = datetime(2026, 4, 16)
    bbox = 50.8218, 34.5952, 50.931, 34.685
    timestamp = '202604152129'
    satellite = 'snpp'
    product='46A3'
    list_available_products()
    #asyncio.run(fetch_winner(timestamp=timestamp, satellite=satellite, product=product, bbox=bbox))
    # print(agent)
    # fpath = '/tmp/VJ246A1.A2026105.h23v05.002.2026106182536.h5'
    # create_vrt_from_local(h5_path=fpath)

