import os.path
import aiofiles
import obstore
from datetime import datetime, timedelta
from rich.progress import Progress
import asyncio
import re
import random
from typing import Iterable
from urllib.parse import urlparse
from ntl.cmask import bbox_in_hdf
# Universal config for anonymous public access
public_config = {"skip_signature": "true"}
import logging
logger = logging.getLogger(__name__)

PRODUCTS_RE = {
    'CM': re.compile(r'^(?P<product>[\w-]+)_(?P<version>v\d+r\d+)_(?P<platform>\w+)_s(?P<start>\d+)_e(?P<end>\d+)_c(?P<creation>\d+)\.(?P<ext>\w+)$'),
    'GEO': re.compile(r'^(?P<product>[^_]+)_(?P<platform>[^_]+)_d(?P<date>\d{8})_t(?P<start>\d+)_e(?P<end>\d+)_b(?P<orbit>\d+)_c(?P<creation>\d+)_(?P<facility>[^_]+)_(?P<env>[^_]+)\.(?P<ext>\w+)$'),
    'SDR': re.compile(r'^(?P<product>[^_]+)_(?P<platform>[^_]+)_d(?P<date>\d{8})_t(?P<start>\d+)_e(?P<end>\d+)_b(?P<orbit>\d+)_c(?P<creation>\d+)_(?P<facility>[^_]+)_(?P<env>[^_]+)\.(?P<ext>\w+)$')
}


PRODUCTS={
    'SDR':"VIIRS-DNB-SDR",
    'GEO':"VIIRS-DNB-GEO",
    'CM':"VIIRS-JRR-CloudMask"
}


PRODUCT_NAMES = tuple(PRODUCTS)
SOURCES = dict(aws='aws', gcp='gcp')
SOURCE_NAMES=tuple(SOURCES)
# Define the base URLs for the three VIIRS satellites
viirs_urls = {
    "SNPP": {
        "aws": "s3://noaa-nesdis-snpp-pds/",
        "gcp": "gs://gcp-noaa-nesdis-snpp/"
    },
    "N20": {
        "aws": "s3://noaa-nesdis-n20-pds/",
        "gcp": "gs://gcp-noaa-nesdis-n20/"
    },
    "N21": {
        "aws": "s3://noaa-nesdis-n21-pds/",
        "gcp": "gs://gcp-noaa-nesdis-n21/"
    }
}

# The "Solid" way: Generate stores using from_url
viirs_stores = {
    sat: {
        source: obstore.store.from_url(url, config=public_config)
        for source, url in sources.items()
    }
    for sat, sources in viirs_urls.items()
}


def parse_noaa_timestamp(time_str: str) -> datetime:
    """
    Converts a NOAA VIIRS string (e.g., '202604010001018') into a timezone-naive UTC datetime.
    """
    # The first 14 characters: YYYYMMDDHHMMSS
    base_time = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")

    # The 15th character: tenths of a second (1 tenth = 100,000 microseconds)
    tenths = int(time_str[14:])

    return base_time + timedelta(microseconds=tenths * 100000)

def public_url(file_path:str=None, satellite:str=None, source:str=None):

    public_cloud_url = viirs_urls[satellite][source]
    parsed_public_url = urlparse(public_cloud_url)
    bucket = parsed_public_url.netloc
    if parsed_public_url.scheme == 's3':
        return f'https://{bucket}.s3.amazonaws.com/{file_path}'
    if parsed_public_url.scheme == 'gs':
        return f'https://storage.googleapis.com/{bucket}/{file_path}'


async def fetch_file(satellite:str=None, provider:str=None, path:str=None, size:int=None, dst_dir:str=None,
                     progress=None, progress_task = None):
    try:
        adir = os.path.abspath(dst_dir)
        if not os.path.exists(adir):
            os.mkdir(adir)
        down_task = None
        store = viirs_stores[satellite][provider]
        fname = os.path.basename(path)
        if progress:
            down_task = progress.add_task(f'[red]Downloading  {fname} from {provider}', total=size)
        dst_file_path = os.path.join(adir, fname)
        response = await obstore.get_async(store, path)
        async with aiofiles.open(dst_file_path, 'wb') as local_file:
            # The 'get' call is the async request
            async for chunk in  response.stream():
                await local_file.write(chunk)
                if progress and down_task is not None:
                    progress.update(down_task, advance=len(chunk))

        if os.stat(dst_file_path).st_size == size:
            if progress and progress_task is not None:
                progress.update(progress_task, description=f'[green]Downloaded {fname} from {provider}', advance=1)
            return dst_file_path, size
    except Exception:

        raise
    finally:
        if progress:
            if down_task:progress.remove_task(down_task)





async def locate_file(satellite:str=None, dt=None, source:str=None, products: Iterable[str] = PRODUCT_NAMES):
    found = {}
    stores = viirs_stores[satellite]

    # 1. Safely determine the primary and alternate sources
    primary_source = source if source else random.choice(SOURCE_NAMES)
    alt_source = SOURCE_NAMES[0] if primary_source == SOURCE_NAMES[1] else SOURCE_NAMES[1]
    entries_cache = {}
    for product_name in products:
        match_found = False
        product = PRODUCTS[product_name]
        sources_to_try = [primary_source, alt_source]
        time_pattern = dt.strftime('s%Y%m%d%H%M' if 'cloud' in product.lower() else 'd%Y%m%d_t%H%M')
        for current_source in sources_to_try:
            store = stores[current_source]

            date_path = dt.strftime('/%Y/%m/%d/')
            prefix = f"{product}{date_path}"
            cache_key = (current_source, prefix)
            if cache_key not in entries_cache:
                try:
                    entries_cache[cache_key] = await obstore.list(store, prefix=prefix).collect_async()
                except Exception as e:
                    logger.warning(f"Failed to list {prefix} from {current_source}: {e}")
                    entries_cache[cache_key] = []

            entries = entries_cache[cache_key]
            if not entries:
                continue
            match_gen = (
                e for e in entries
                if time_pattern in e['path'] and e['path'].lower().endswith(('.nc', '.h5'))
            )

            # next() takes the first match, or returns None if the generator is empty
            selected_entry = next(match_gen, None)

            if selected_entry:
                file_path = selected_entry['path']
                file_size = selected_entry.get('size', 0)  # Safe get
                if current_source not in found:  # reset
                    found[current_source] = []
                found[current_source].append((file_path, file_size))
                break  # Found it! Stop looking in fallback sources for this product

            else:
                logger.debug(f"Pattern {time_pattern} not found in {current_source} for {product_name}")


    return found

async def find_ntl(satellite: str = None, bbox: Iterable[float] = None, dt: datetime = None,
                   products: Iterable[str] = PRODUCT_NAMES, source: str = None):
    found = {}
    stores = viirs_stores[satellite]

    # 1. Safely determine the primary and alternate sources
    primary_source = source if source else random.choice(SOURCE_NAMES)
    alt_source = SOURCE_NAMES[0] if primary_source == SOURCE_NAMES[1] else SOURCE_NAMES[1]

    # Calculate target times upfront to handle rollovers safely
    target_dts = [dt, dt - timedelta(minutes=1), dt + timedelta(minutes=1)]
    for product_name in products:
        product = PRODUCTS[product_name]
        sources_to_try = [primary_source, alt_source]

        # Track time patterns that we know missed the bounding box
        spatial_misses = set()
        match_found = False

        for current_source in sources_to_try:
            store = stores[current_source]
            entries_cache = {}

            for sc_dt in target_dts:
                # Format the time pattern dynamically based on the specific offset
                time_pattern = sc_dt.strftime('s%Y%m%d%H%M' if 'cloud' in product.lower() else 't%H%M')

                # Instantly skip if we already proved this timestamp misses the bbox on a previous source
                if time_pattern in spatial_misses:
                    continue

                date_path = sc_dt.strftime('/%Y/%m/%d/')
                prefix = f"{product}{date_path}"

                # Cache listed entries by prefix so we don't make duplicate network calls
                if prefix not in entries_cache:
                    entries_cache[prefix] = await obstore.list(store, prefix=prefix).collect_async()

                entries = entries_cache[prefix]

                if not entries:
                    continue

                try:
                    selected_entry = [e for e in entries if time_pattern in e['path'] and (
                                e['path'].endswith('.nc') or e['path'].endswith('.h5'))].pop()
                    file_path, file_size = selected_entry['path'], selected_entry['size']
                    public_file_url = public_url(file_path=file_path, satellite=satellite, source=current_source)
                    if not bbox_in_hdf(hdf_url=public_file_url,bbox=bbox):
                        logger.debug(
                            f'Skipping {file_path} from {current_source} generated by {satellite} because it does not intersect {bbox} bbox')
                        spatial_misses.add(time_pattern)
                        continue

                    if current_source not in found: #reset
                        found[current_source] = []

                    found[current_source].append((file_path, file_size))
                    match_found = True
                    break

                except IndexError:
                    logger.debug(
                        f'No exact match for satellite {satellite} timestamp {time_pattern} on {store}. Considering temporal neighbors.')

                    continue

            if match_found:
                break

        if not match_found:
            logger.debug(
                f'No valid/intersecting data for product {product} and satellite {satellite} for the night {dt}')


    return found
async def fetch_ntl(found_paths:dict[str, list]=None, satellite:str=None, dst_dir='/tmp', progress=None):

    # Download logic (Surgical io to local SSD)
    tasks = []
    progress_task = None
    try:

        #logger.info(f'Downloading VIIRS images...')
        async with asyncio.TaskGroup() as tg:
            for provider, files in found_paths.items():
                if progress:
                    progress_task = progress.add_task(description=f'Downloading VIIRS images...', total=len(files))
                for path, size in files:
                    tasks.append(tg.create_task(fetch_file(
                        satellite=satellite, provider=provider,
                        path=path, size=size, progress=progress,
                        dst_dir=dst_dir, progress_task = progress_task
                    )))
    except ExceptionGroup as eg:
        for e in eg.exceptions:
            logger.error(f"❌ Sub-task failed: {e}")
    finally:
        if progress and progress_task is not None:
            progress.remove_task(progress_task)
        return dict([t.result() for t in tasks])





async def find_and_fetch_ntl(
        satellite:str=None, dt:datetime=None,
        products:Iterable[str]=PRODUCT_NAMES, dst_dir='/tmp'
):
    found_paths = await find_ntl(satellite=satellite, dt=dt, products=products)
    return await fetch_ntl(found_paths=found_paths,satellite=satellite, dst_dir=dst_dir)


