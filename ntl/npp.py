import math
import logging
import re
import sys
from enum import Enum
from dotenv import load_dotenv
import argparse
import httpx
from dotenv import load_dotenv
import argparse
from datetime import datetime, date, time, timedelta, timezone
from typing import List, Tuple, Dict, Optional
from pathlib import Path
import asyncio
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
NRT_ROOT = "https://nrt3.modaps.eosdis.nasa.gov"
STD_ROOT = "https://ladsweb.modaps.eosdis.nasa.gov"

class AnalysisTier(Enum):
    RAPID_ONSET = "rapid_onset"
    REFINED_TARGET = "refined_target"
    def __str__(self):
        return self.value



def get_search_priority(tier: AnalysisTier):
    if tier == AnalysisTier.RAPID_ONSET:
        return ["VNP46A2_NRT", "VNP46A1_NRT", "VNP46A1"]

    if tier == AnalysisTier.REFINED_TARGET:
        # Strict requirement for lunar/atmospheric correction
        return ["VNP46A2", "VNP46A2_NRT"]

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


def tile_scanned(h: int, target_date: date) -> bool:
    """
    Checks if the 01:30 AM overpass for a specific tile and date has
    physically occurred yet.
    """

    current_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    # 1. Calculate approximate center longitude of the tile
    # h00 is -180, h18 is 0. Center is +5 degrees into the 10-degree slice.
    lon = (h * 10) - 180 + 5

    # 2. Calculate UTC offset (1 hour per 15 degrees)
    # Positive lon (East) means UTC is EARLIER than Local.
    # Negative lon (West) means UTC is LATER than Local.
    utc_offset_hours = lon / 15.0

    # 3. Create the Local Overpass object (01:30 AM)
    local_overpass = datetime.combine(target_date, time(1, 30))

    # 4. Convert Local Overpass to UTC
    # UTC = Local - Offset
    overpass_utc = local_overpass - timedelta(hours=utc_offset_hours)

    # Logic check: Has the current time passed the overpass time?
    is_scanned = current_utc >= overpass_utc

    return is_scanned, overpass_utc


def construct_nasa_url(prod, nasa_folder_date):
    """
    Constructs the directory URL for a specific product, date, and tile.

    prod: 'VNP46A1_NRT', 'VNP46A2_NRT', or 'VNP46A1'
    nasa_folder_date: date object (already adjusted by K-Offset)
    h, v: tile coordinates (integers)
    """
    year = nasa_folder_date.year
    doy = nasa_folder_date.strftime('%j')  # Day of Year (001-366)



    # 1. Determine the Base Server
    # NRT data is usually on nrt3/nrt4; Archive is on ladsweb
    if "_NRT" in prod:
        #base = "https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/allData/5000"
        base = "https://nrt3.modaps.eosdis.nasa.gov/archive/allData/5200"
    else:
        base = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200"

    # 2. Build the Directory Path
    # Structure: base / product / year / doy
    directory_url = f"{base}/{prod}/{year}/{doy}"

    return directory_url


async def fetch_ntl_data(target_date:date = None, h:int=None, v:int=None,
                         max_concurrency=6, dst_dir:str|Path = None, tier:AnalysisTier=None):
    is_scanned, overpass_utc = tile_scanned(h=h, target_date=target_date)
    if not is_scanned:
        logger.info(f"Tile h{h}v{v}: Satellite hasn't arrived yet.")
        return



    # 2. CALCULATE "AGE" (T)
    # T = How many hours ago did the click happen?
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    delta = (now_utc - overpass_utc).total_seconds() / 3600


    # 3. THE NASA FOLDER SEAM (K-Offset)
    k = -1 if h >= 20 else 0
    nasa_folder_date = target_date + timedelta(days=k)
    year = nasa_folder_date.year
    doy = nasa_folder_date.strftime('%j')
    tile = f"h{h:02d}v{v:02d}"
    logger.debug(f'Tile {h}::{v} is scanned? : {is_scanned} {overpass_utc}')
    products = get_search_priority(tier=tier)

    async with httpx.AsyncClient() as client:
        if tier == AnalysisTier.RAPID_ONSET:
            # Don't check A1 (Archive/NRT) if data is less than 3h old (downlink lag)
            if delta < 3:
                return
            for prod in products:
                # --- Smart Efficiency Gate ---
                # Don't check A2_NRT if data is less than 15h old (it won't be there)
                if prod == 'VNP46A2_NRT' and delta < 15:
                    continue

                # 1. Determine the Discovery API URL (The "Light Switch")
                # We use the API for EVERYTHING to avoid guessing filenames/folders
                if "_NRT" in prod:
                    api_base = "https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/details/allData/5200"
                else:
                    api_base = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details/allData/5200"

                discovery_url = f"{api_base}/{prod}/{year}/{doy}"

                # 2. Fetch the Directory Content
                response = await client.get(discovery_url)

                # CRITICAL FIX: Check for integer 200, not string '200'
                if response.status_code == 200:
                    data = response.json()
                    files = data.get('content', [])

                    # 3. Filter for your specific Tile
                    # This handles .001, .002, and timestamps automatically
                    matches = [
                        f for f in files
                        if tile in f['name'] and f['name'].endswith('.h5')
                    ]

                    if matches:
                        # Sort by name to get the latest version if multiple exist
                        matches.sort(key=lambda x: x['name'], reverse=True)

                        # The API returns 'downloadsLink' for the full URL and 'size' for bytes
                        return matches[0]['downloadsLink'], matches[0]['size']

                # If we hit a 404 here, it means the FOLDER (Day 102/103) is missing
                logger.warning(f"⚠️ Folder {doy} for {prod} not found on server.")
        if tier == AnalysisTier.REFINED_TARGET:
            if delta < 20:
                logger.warning(f'There is no data available to conduct a refined target analysis for {target_date} ')
                return
            for prod in products:
                if "_NRT" in prod:
                    # Near Real-Time Discovery API (Collection 5200)
                    api_base = "https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/details/allData/5200"
                else:
                    # Standard Archive Discovery API (Collection 5200)
                    api_base = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details/allData/5200"
                discovery_url = f"{api_base}/{prod}/{year}/{doy}"

                # 2. Fetch the Directory Content
                response = await client.get(discovery_url)


                # CRITICAL FIX: Check for integer 200, not string '200'
                if response.status_code == 200:
                    data = response.json()
                    files = data.get('content', [])

                    # 3. Filter for your specific Tile
                    # This handles .001, .002, and timestamps automatically
                    matches = [
                        f for f in files
                        if tile in f['name'] and f['name'].endswith('.h5')
                    ]

                    if matches:
                        # Sort by name to get the latest version if multiple exist
                        matches.sort(key=lambda x: x['name'], reverse=True)

                        # The API returns 'downloadsLink' for the full URL and 'size' for bytes
                        return matches[0]['downloadsLink'], matches[0]['size']

class Formatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ntl",
        description="RAPIDA NTL Impact Engine",
        formatter_class=Formatter,

    )
    parser.add_argument('--date', type=date.fromisoformat, default='2026-04-13',
                        help="Target date in ISO format (YYYY-MM-DD)")

    parser.add_argument('--bbox', type=float, nargs=4,
                        metavar=('LON_min', 'LAT_MIN', 'LON_MAX', 'LAT_MAX'),
                        help="Bounding box coordinates")

    parser.add_argument('--tier', type=AnalysisTier, choices=list(AnalysisTier),
                        default=AnalysisTier.RAPID_ONSET,
                        help="Analysis tier: rapid_onset (48h) or refined_target (72h)")

    parser.add_argument(
        "--dst-folder",
        type=str,
        required=False,
        metavar='PATH',
        help="A path to a directory on the local disk where the NTL files will be stored. ",
    )

    parser.add_argument(
        "-d", "--debug", help="Enable debug logging", action="store_true"
    )

    return parser

async def main(argv: list[str] | None = None) -> int:

    load_dotenv()
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.getLogger('httpx').setLevel(logging.DEBUG)
    logger.name = 'ntl'
    # target_date = '2026-04-13'
    # target_bbox = (14.0, 48.5, 19.0, 51.0)  # Example Bounding Box
    # target_bbox = ([-82.0, 33.0, -75.0, 41.0])  # Example Bounding Box
    # target_date = date.fromisoformat(target_date)
    # for h in range(36):
    #     asyncio.run(fetch_ntl_data(target_date=target_date, h=h, v=13, tier=AnalysisTier.RAPID_ONSET))
    parser = build_parser()

    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        parser.print_help()
        return 1

    args = parser.parse_args(argv)

    if args.debug:
        logger.setLevel(logging.DEBUG)

    tiles = get_intersecting_tiles(bbox=args.bbox)

    for h, v in tiles:
        r = await fetch_ntl_data(target_date=args.date,h=h, v=v,tier=args.tier)
        logger.info(r)

    return 0

if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]))