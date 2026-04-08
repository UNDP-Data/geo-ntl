import asyncio
import math
import os
from pathlib import Path
from typing import List, Tuple
import logging
import httpx

from rich.progress import Progress

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





# ---------------------------------------------------------
# Example Execution Context (How the main app calls this)
# ---------------------------------------------------------
async def run_rapida_ingestion():
    APP_KEY = os.getenv("EARTH_ACCESS_TOKEN")
    CRISIS_BBOX = (14.0, 48.5, 19.0, 51.0)  # Example Bounding Box

    # Define custom timeout and connection pooling
    timeout = httpx.Timeout(10.0, read=30.0)
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)

    # Initialize the client ONCE, injecting the AppKey into the global headers
    headers = {"Authorization": f"Bearer {APP_KEY}"}

    async with httpx.AsyncClient(headers=headers, timeout=timeout, limits=limits) as client:
        await fetch_alert_tiles(
            bbox=CRISIS_BBOX,
            year=2026,
            doy=94,
            client=client,
            dest_dir="../rapida_data/alert",
            max_concurrency=6  # Adjust based on LANCE server limits
        )


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    import asyncio
    logging.basicConfig()
    logger = logging.getLogger('ntl')
    logger.setLevel(logging.INFO)
    asyncio.run(run_rapida_ingestion())
