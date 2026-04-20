import os.path
import aiofiles
import obstore
from datetime import datetime
from rich.progress import Progress
import asyncio

from ntl.orb_search import logger

# Universal config for anonymous public access
public_config = {"skip_signature": "true"}
import logging
logger = logging.getLogger(__name__)

# Define the base URLs for the three VIIRS satellites
viirs_urls = {
    "SUOMI NPP": {
        "aws": "s3://noaa-nesdis-snpp-pds/",
        "gcp": "gs://gcp-noaa-nesdis-snpp/"
    },
    "NOAA 20": {
        "aws": "s3://noaa-nesdis-n20-pds/",
        "gcp": "gs://gcp-noaa-nesdis-n20/"
    },
    "NOAA 21": {
        "aws": "s3://noaa-nesdis-n21-pds/",
        "gcp": "gs://gcp-noaa-nesdis-n21/"
    }
}

# The "Solid" way: Generate stores using from_url
viirs_stores = {
    sat: {
        provider: obstore.store.from_url(url, config=public_config)
        for provider, url in providers.items()
    }
    for sat, providers in viirs_urls.items()
}


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



async def find_and_fetch_ntl(satellite:str=None, dt:datetime=None):
    """
    date_path: "2026/04/11"
    ststr: "t2156485"
    """
    targets = ["VIIRS-DNB-SDR", "VIIRS-DNB-GEO"]
    found_paths = {}
    # Usage:
    stores = viirs_stores[satellite]

    date_path = dt.strftime(f'/%Y/%m/%d/')

    for provider, store in stores.items():

        for product in targets:
            prefix = f"{product}/{date_path}/"
            all_entries = await obstore.list(store, prefix=prefix).collect_async()
            # Now filter for your timestamp 't2156'
            time_pattern = dt.strftime(f't%H%M')

            try:
                target_file = [e for e in all_entries if time_pattern in e['path'] and e['path'].endswith('.h5')].pop()
                if not provider in found_paths:
                    found_paths[provider] = []
                found_paths[provider].append((target_file['path'], target_file['size']))
            except IndexError:
                pass
        if not found_paths or len(found_paths[provider]) != 2:
            found_paths = {}
            continue
        break


    # Download logic (Surgical fetch to local SSD)
    tasks = []
    with Progress(disable=False, console=None, transient=False) as progress:
        try:
            async with asyncio.TaskGroup() as tg:
                for provider, files in found_paths.items():
                    for path, size in files:
                        tasks.append(tg.create_task(fetch_file(
                            satellite=satellite, provider=provider,
                            path=path, size=size, progress=progress,
                            dst_dir='/tmp'
                        )))
        except ExceptionGroup as eg:
            for e in eg.exceptions:
                logger.error(f"❌ Sub-task failed: {e}")
    return [t.result() for t in tasks]



