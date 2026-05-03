import math
from datetime import datetime, date, time, timedelta
from pystac_client import Client
import aiofiles
import json
from urllib.parse import urlparse
from dotenv import load_dotenv
import httpx
import os
from pathlib import Path
from rich.progress import Progress
import asyncio
import logging

load_dotenv()

logger = logging.getLogger(__name__)



PROCESSING_LEVELS = [f'A{i}' for i in range(1,5)]
PRODUCT = 46

COLLECTIONS_STRING = \
'''
{
  "LANCEMODIS": {
    "A1": [
      "VJ146A1_NRT_2",
      "VNP46A1_NRT_1",
      "VNP46A1_NRT_2"
    ],
    "A1G": [
      "VJ146A1G_NRT_2",
      "VNP46A1G_NRT_2",
      "VNP46A1G_NRT_1"
    ],
    "A2": [
      "VNP46A2_NRT_2"
    ]
  },
  "LAADS": {
    "A1": [
      "VJ146A1_2",
      "VNP46A1_2"
    ],
    "A2": [
      "VJ146A2_2",
      "VNP46A2_2"
    ],
    "A3": [
      "VJ146A3_2",
      "VNP46A3_2",
      "VNP46A3_1"
    ],
    "A4": [
      "VJ146A4_2",
      "VNP46A4_2",
      "VNP46A4_1"
    ]
  }
}
'''
COLLECTIONS = json.loads(COLLECTIONS_STRING)
LAADS = 'LAADS'
LANCEMODIS = 'LANCEMODIS'
STD = 'STD'
NRT = 'NRT'
STREAMS = NRT, STD
CATALOGS = LANCEMODIS, LAADS
STREAM2CATALOG = dict(zip(STREAMS, CATALOGS))
CATALOG2STREAM = {value: key for key, value in STREAM2CATALOG.items()}
CMR_STAC_ROOT = 'https://cmr.earthdata.nasa.gov/stac/'

def generate_collections(catalogs=CATALOGS, product_filter=PRODUCT ):
    collections = {}
    for catalog_name in catalogs:
        catalog_url = f'{CMR_STAC_ROOT}{catalog_name}'
        catalog = Client.open(catalog_url)

        for collection in catalog.get_collections():
            if f'{product_filter}' in collection.id:
                if not catalog_name in collections:
                    collections[catalog_name] = {}
                parts = collection.id.split('_')
                if len(parts) == 3:  # NRT
                    product, stream, version = parts
                    level = product[5:]
                elif len(parts) == 2:  # STD
                    product, version = parts
                    level = product[5:]
                if not level in collections[catalog_name]:
                    collections[catalog_name][level] = []
                collections[catalog_name][level].append(collection.id)
    return collections


def search(stream:str, processing_level:str, dt:datetime, bbox:tuple[float]):

    catalog_name = STREAM2CATALOG[stream]
    catalog_collections = COLLECTIONS[catalog_name]
    catalog_processing_levels = catalog_collections.keys()
    assert processing_level in catalog_processing_levels, (f'Invalid processing level {processing_level} for {catalog_name}. \''
                                                           f'Valid processing levels {catalog_processing_levels}')

    available_collections = catalog_collections[processing_level]

    logger.info(f'Searching imagery in catalog "{catalog_name}" collections: {available_collections}')
    logger.debug(f'Searching for {processing_level} imagery in catalog "{catalog_name}" collections: {available_collections} ' \
                 f'for {dt} and {bbox} geaographic area')
    stac_url = f'{CMR_STAC_ROOT}{catalog_name}'
    catalog = Client.open(url=stac_url)
    search = catalog.search(
        collections=[available_collections],
        datetime=dt,
        bbox=bbox
    )
    logger.info(f"Found {search.matched()} standard granule(s).")
    if search.matched():
        items = search.item_collection()
        urls = []
        for itm in items:
            for asset_key, asset in itm.assets.items():
                # Look for the .h5 file, but specifically grab the HTTPS link
                if asset.href.endswith('.h5') and asset.href.startswith('https'):
                    urls.append(asset.href)

        return urls


async def download_file(client:httpx.AsyncClient, url:str, semaphore:asyncio.Semaphore, progress:Progress,
                        dst_file_path: Path, max_retries:int=3,):

    async with semaphore:
        for attempt in range(max_retries):
            try:
                # Start the streaming request
                async with client.stream("GET", url) as response:
                    response.raise_for_status()

                    # Initialize progress bar for this specific file
                    file_size = int(response.headers.get("Content-Length", 0))
                    if progress:
                        down_task = progress.add_task(f'[red]Downloading  {dst_file_path.name}', total=file_size)

                    tmp_path = dst_file_path.with_suffix(dst_file_path.suffix + ".tmp")
                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            await f.write(chunk)
                            if progress:
                                progress.advance(down_task, advance=len(chunk))
                    tmp_path.rename(dst_file_path)

                if dst_file_path.stat().st_size == file_size:
                    if progress:
                        progress.update(down_task, description=f'[green]Downloaded {dst_file_path.name}')
                    return dst_file_path

            except Exception as e:
                logger.error(e)
                if attempt == max_retries - 1:
                    msg = f'Failed to download {dst_file_path.name}'
                    if progress:
                        progress.update(down_task, description=f"[red]✗{msg} ")
                    raise  e
                if progress:
                    progress.remove_task(down_task)
                # Exponential backoff before retry (1s, 2s, 4s...)
                await asyncio.sleep(2 ** attempt)


async def download(urls: list[str], dst_dir: str, max_concurrency=5, progress: Progress = None):
    # 1. Fail fast if token is missing
    ea_token = os.environ.get('EARTHDATA_TOKEN')
    if not ea_token:
        raise ValueError("EARTHDATA_TOKEN environment variable is not set!")

    headers = {"Authorization": f"Bearer {ea_token}"}
    dest_path = Path(dst_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = []

    # Sets connect, write, and pool to 60.0, but allows 300.0 for read
    timeout_config = httpx.Timeout(60.0, read=300.0)

    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=timeout_config) as client:
        for url in urls:
            # 3. Cleaner filename extraction
            file_name = Path(urlparse(url).path).name
            dst_file_path = dest_path / file_name

            task = asyncio.create_task(
                download_file(
                    client=client,
                    url=url,
                    semaphore=semaphore,
                    dst_file_path=dst_file_path,
                    progress=progress
                ),
                name=file_name
            )
            tasks.append(task)

        # 4. Use gather with return_exceptions=True.
        # This waits for ALL tasks to finish (or fail) without crashing the batch.
        results = await asyncio.gather(*tasks, return_exceptions=True)

    downloaded_files = []

    # 5. Process results safely
    for task_obj, result in zip(tasks, results):
        file_name = task_obj.get_name()

        if isinstance(result, Exception):
            # Log the error, but let the rest of the downloaded files proceed
            logger.error(f"Failed to download {file_name}: {result}")
        elif result:  # Assuming download_file returns the path on success
            downloaded_files.append(str(result))

    return downloaded_files


def calculate_night_hours(midlat: float, day_of_year: int) -> int:
    """
    Calculates the average hours of nighttime for a given latitude and Julian day.
    """
    # 1. Approximate Solar Declination (in radians)
    # 23.44 represents Earth's axial tilt. 81 is approx the Spring Equinox.
    tilt = math.radians(23.44)
    angle = math.radians((360 / 365.24) * (day_of_year - 81))
    declination = math.asin(math.sin(tilt) * math.sin(angle))

    # 2. Your Hour Angle math
    lat_rad = math.radians(midlat)
    cos_h = -math.tan(lat_rad) * math.tan(declination)

    # Clamp to [-1, 1] to handle Polar Day (0 night) and Polar Night (24h night)
    cos_h = max(-1.0, min(1.0, cos_h))

    # Calculate hours
    daylight_hrs = 2 * math.degrees(math.acos(cos_h)) / 15
    night_hrs = 24 - daylight_hrs

    return int(round(night_hrs))

import h5py

def filter_images(image_paths:list[str], bbox:tuple[float], min_elevation_angle=20, min_cloud_cover=20):
    with h5py.File(image_paths[0], 'r') as f:
        zenith_dataset = f['HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/Sensor_Zenith']

        bounds = [round(float(f.attrs[k][0])) for k in ('WestBoundingCoord', 'SouthBoundingCoord', 'EastBoundingCoord', 'NorthBoundingCoord')]




async def fetch(stream:str, processing_level:str, target_date:datetime, bbox:tuple[float],
                max_concurrency=5, dst_dir:str=None, progress:Progress=None):
    minlon, minlat, maxlon, maxlat = bbox



    midlat = (minlat + maxlat) * .5
    night_hrs = calculate_night_hours(midlat,day_of_year=int(target_date.strftime('%j')))

    start_dt = target_date - timedelta(hours=night_hrs/2)
    end_dt = target_date + timedelta(hours=night_hrs/2)

    dt = [start_dt, end_dt]
    if 'A3' in processing_level:
        dt = datetime(year=target_date.year, month=target_date.month, day=15)

    downloaded_files = []
    if progress:
        progress_task = progress.add_task(description=f'[red]Searching and downloading VIIRS DNB imagery for {target_date}', total=None)
    urls = search(
        stream=stream,
        processing_level=processing_level,
        dt=dt,
        bbox=bbox,
    )
    if progress:
        progress.update(progress_task, description=f'[red]Found {len(urls)} image(s)...')
    if urls:
        downloaded_files = await download(urls=urls,dst_dir=dst_dir, progress=progress, max_concurrency=max_concurrency)
        if progress:
            progress.update(progress_task, description=f'[red]Downloaded {len(downloaded_files)} image(s)...', total=100, completed=100)
    else:
        logger.info(f'No imagery')


    return downloaded_files

if __name__ == '__main__':
    import asyncio
    import logging
    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.INFO,  # Or whatever level you use
        format="%(message)s",  # RichHandler handles the timestamps and formatting natively
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)]
    )
    logger = logging.getLogger()
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logger.name = 'ntl'
    logger.setLevel(logging.INFO)
    datetime_range = "2026-03-14/2026-03-16"
    datetime_str = "2026-04-30"
    target_date = datetime(2026, 1, 12)
    bbox = [50.93291451, 35.31478752, 51.81675159, 35.99372355]
    bbox = -98.10, 30.05, -97.40, 30.55
    stream = STD

    with Progress(disable=False, transient=False) as progress:
        asyncio.run(
            fetch(stream=stream,
                  processing_level='A3',
                  target_date=target_date,
                  bbox=bbox,
                  dst_dir='/tmp',
                  progress=progress)
        )
    # images = [e.path for e in os.scandir('/tmp') if e.name.endswith('.h5')]
    # filter_images(image_paths=images, bbox=bbox)



