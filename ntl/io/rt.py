import os.path
import aiofiles
import obstore
from datetime import datetime, timedelta
from rich.progress import Progress
import asyncio
import re
from typing import Iterable
from urllib.parse import urlparse
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


async def fetch_file(satellite:str=None, provider:str=None, path:str=None, size:int=None, dst_dir:str=None, progress=None):
    try:
        adir = os.path.abspath(dst_dir)
        if not os.path.exists(adir):
            os.mkdir(adir)

        store = viirs_stores[satellite][provider]
        fname = os.path.basename(path)
        down_task = progress.add_task(f'[red]Downloading {satellite} NTL image {fname} from {provider}', total=size)
        dst_file_path = os.path.join(adir, fname)
        response = await obstore.get_async(store, path)
        async with aiofiles.open(dst_file_path, 'wb') as local_file:
            # The 'get' call is the async request
            async for chunk in  response.stream():
                await local_file.write(chunk)
                progress.update(down_task, advance=len(chunk))

        if os.stat(dst_file_path).st_size == size:
            progress.update(down_task, advance=len(chunk), description=f'[green]Downloaded {fname} from {provider}')
            return dst_file_path
    except Exception:
        if down_task:
            progress.remove_task(down_task)
        raise



async def find_ntl(satellite:str=None, dt:datetime=None,
        products:Iterable[str]=PRODUCT_NAMES, source:str=None ):
    found = {}
    stores = viirs_stores[satellite]
    date_path = dt.strftime(f'/%Y/%m/%d/')
    if source:
        stores = {source: stores[source]}

    for source, store in stores.items():

        for product_name in products:
            product = PRODUCTS[product_name]
            prefix = f"{product}/{date_path}/"
            all_entries = await obstore.list(store, prefix=prefix).collect_async()
            # Now filter for your timestamp 't2156'
            time_pattern = dt.strftime(f't%H%M')
            if 'cloud' in product.lower():
                time_pattern = dt.strftime(f's%Y%m%d%H%M')
            try:
                if not source in found:
                    found[source] = []
                target_file = [e for e in all_entries if time_pattern in e['path'] and (e['path'].endswith('.nc') or e['path'].endswith('.h5'))].pop()

                found[source].append((target_file['path'], target_file['size']))
            except IndexError as ie:
                logger.info(f'No exact match was detected for satellite {satellite} timestamp {time_pattern} on {store}. Considering neighbors.')
                rex = PRODUCTS_RE[product_name]
                for e in all_entries:
                    rel_path, fname = os.path.split(e['path'])
                    m = rex.match(fname)
                    if m:
                        file_vars = m.groupdict()
                        starts, ends = file_vars['start'], file_vars['end']
                        startt, endt = parse_noaa_timestamp(starts), parse_noaa_timestamp(ends)
                        if dt.minute//10 == startt.minute//10:
                            if startt <= dt <= endt:
                                found[source].append((e['path'], e['size']))

                continue

        if not found or len(found[source]) != len(products):
            found = {}
            continue
        break
    if len(found) != 1:
        raise Exception(f'Failed to locate NTL data in {stores} {found}')
    (source, paths), = found.items()
    if len(paths) != len(products):
        raise Exception(f'Incorrect number of files was collected from {source}')

    return found


async def fetch_ntl(found_paths:dict[str, list]=None, satellite:str=None, dst_dir='/tmp'):

    # Download logic (Surgical io to local SSD)
    tasks = []
    with Progress(disable=False, console=None, transient=False) as progress:
        try:
            async with asyncio.TaskGroup() as tg:
                for provider, files in found_paths.items():
                    for path, size in files:
                        tasks.append(tg.create_task(fetch_file(
                            satellite=satellite, provider=provider,
                            path=path, size=size, progress=progress,
                            dst_dir=dst_dir
                        )))
        except ExceptionGroup as eg:
            for e in eg.exceptions:
                logger.error(f"❌ Sub-task failed: {e}")
    return [t.result() for t in tasks]


async def find_and_fetch_ntl(
        satellite:str=None, dt:datetime=None,
        products:Iterable[str]=PRODUCT_NAMES, dst_dir='/tmp'
):
    found_paths = await find_ntl(satellite=satellite, dt=dt, products=products)
    return await fetch_ntl(found_paths=found_paths,satellite=satellite, dst_dir=dst_dir)