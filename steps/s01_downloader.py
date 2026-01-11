"""
async downloader for geospatial raster data with caching mechanism.
downloads tiles asynchronously using aiohttp and aiofiles.
"""
import os
import asyncio # asynchronous programming
import aiohttp # async http client
import aiofiles # async file operations
import logging
import time
from typing import Dict, Any, List

from aiohttp import ClientSession
from tqdm.asyncio import tqdm
from dataclasses import dataclass
from config import BASE_DIR, ANALYSE_BBOX, LayerConfig, DOWNLOAD_LAYERS
import ssl
from datetime import datetime, timedelta

# Configuration
# ssl context to ignore certificate errors
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# max concurrent requests for aiohttp
MAX_CONCURRENT_REQUESTS = 50

@dataclass
class DownloadTask:
    """
    data class for download task. represents a single tile download job
    """
    url: str
    params: Dict[str, Any]
    filepath: str
    pgw_content: str
    tile_id: str

def get_cache_metadata_file(layer_subdir: str) -> str:
    """
    returns path to cache metadata file (json with download date)

    Args:
        layer_subdir (str): layer subdirectory

    Returns:
        str: path to metadata file
    """
    return os.path.join(layer_subdir, ".cache_metadata.txt")

async def is_cache_valid(layer: LayerConfig, session: ClientSession, cache_age_days: int = 7) -> bool:
    """
    checks if the cache for a given layer is valid based on age and existence of files
    1. checks if directory and files exist
    2. gets modification date of first png file
    3. checks if cache is older then cache_age_days

    Args:
        layer (LayerConfig): layer configuration
        session (ClientSession): aiohttp client session
        cache_age_days (int): maximum age of cache in days (default: 7)

    Returns:
        bool: true if cache is valid, false otherwise
    """
    # setup logger
    logger = logging.getLogger("DOWNLOADER")
    metadata_file = get_cache_metadata_file(layer.subdir)

    # 1. check if directory and files exist
    if not os.path.exists(layer.subdir):
        logger.info(f"📂 Kein Cache für {layer.name} (Verzeichnis existiert nicht)")
        return False

    # count png files
    png_count = len([f for f in os.listdir(layer.subdir) if f.endswith('.png')])
    if png_count == 0:
        logger.info(f"📂 Kein Cache für {layer.name} (Keine Dateien gefunden)")
        return False

    # 2. get modification date of first png file
    try:
        png_files = [f for f in os.listdir(layer.subdir) if f.endswith('.png')]
        if not png_files:
            return False

        first_png = os.path.join(layer.subdir, png_files[0])
        mtime = os.path.getmtime(first_png)
        cache_date = datetime.fromtimestamp(mtime)

        # 3. check if cache is older then cache_age_days
        age = datetime.now() - cache_date
        max_age = timedelta(days=cache_age_days)

        if age <= max_age:
            logger.info(f"✅ Cache gültig für {layer.name} ({age.days} Tage alt, max. {cache_age_days} Tage)")
            # save metadata file as well
            with open(metadata_file, 'w') as f:
                f.write(cache_date.isoformat())
            return True

        else:
            logger.info(f"❌ Cache zu alt für {layer.name} ({age.days} Tage alt, max. {cache_age_days} Tage)")
            return False

    except Exception as e:
        logger.warning(f"⚠️  Konnte Cache-Alter nicht ermitteln für {layer.name}: {e}")
        return False


def save_cache_metadata(layer: LayerConfig) -> None:
    """
    saves cache metadata file with current date

    Args:
        layer (LayerConfig): layer config

    Returns:
        None
    """
    metadata_file = get_cache_metadata_file(layer.subdir)
    with open(metadata_file, 'w') as f:
        f.write(datetime.now().isoformat())

def erstelle_pgw_inhalt(xmin: float, ymin: float, xmax: float, ymax: float, w_px: int, h_px: int) -> str:
    """
    creates the content of a .pgw world file for a given bbox and pixel dimension

    Args:
        xmin float: minimum x coordinate (left)
        ymin float: minimum y coordinate (bottom)
        xmax float: maximum x coordinate (right)
        ymax float: maximum y coordinate (top)
        w_px int: width in pixels
        h_px int: height in pixels

    Returns:
        str: content of the .pgw file
    """
    A = (xmax - xmin) / float(w_px)
    E = -(ymax - ymin) / float(h_px)
    C = xmin + (A / 2.0)
    F = ymax + (E / 2.0)
    return f"{A:.10f}\n0.0\n0.0\n{E:.10f}\n{C:.10f}\n{F:.10f}\n"

async def download_worker(session: aiohttp.ClientSession, task: DownloadTask, semaphore: asyncio.Semaphore) -> bool:
    """
    async download worker for a single tile and uses semaphore to limit concurrent downloads
    1. sends async GET request
    2. checks response status
    3. reads content
    4. plausibility check (file size)
    5. async writes file and pgw

    Args:
        session (aiohttp.ClientSession): aiohttp session
        task (DownloadTask): download task
        semaphore (asyncio.Semaphore): semaphore to limit concurrent downloads

    Returns:
        bool: true if download and save were successful, false otherwise
    """
    logger = logging.getLogger("DOWNLOADER")

    # semaphore blocks process if limit is reached
    async with semaphore:
        try:
            # 1. send async GET reuqest
            async with session.get(task.url, params=task.params, timeout=30) as response:

                # 2. check response status
                if response.status != 200:
                    if response.status == 404:
                        logger.debug(f"Kachel nicht gefunden (404): {task.tile_id}")
                    elif response.status == 429:
                        logger.critical(f"🛑 RATE LIMIT (429) bei {task.tile_id}! Wir sind zu schnell.")
                    else:
                        logger.error(f"❌ HTTP {response.status} bei {task.tile_id}")
                    return False

                # 3. read content
                content = await response.read()

                # 4. check plausibility (file size) -> min 500 bytes
                if len(content) < 500:
                    logger.warning(f"⚠️ Datei zu klein (<500b), ignoriere: {task.tile_id}")
                    return False

                # 4. async write file and pgw
                try:
                    async with aiofiles.open(task.filepath, 'wb') as f:
                        await f.write(content)

                    # pgw file (can be written synchronously as its small even if process is blocked)
                    pgw_path = task.filepath.replace(".png", ".pgw")
                    async with aiofiles.open(pgw_path, 'w') as f:
                        await f.write(task.pgw_content)

                    return True

                except OSError as e:
                    logger.error(f"💾 Schreibfehler {task.filepath}: {e}")
                    return False

        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout bei {task.tile_id}")
            return False
        except aiohttp.ClientError as e:
            logger.error(f"🔌 Netzwerkfehler bei {task.tile_id}: {e}")
            return False
        except Exception as e:
            logger.critical(f"🔥 Crash bei {task.tile_id}: {e}")
            return False

def prepare_tasks(layer: LayerConfig, bbox: Dict) -> List[DownloadTask]:
    """
    prepares download tasks for a given layer and bbox
    1. iterates over bbox in steps of tile size
    2. creates DownloadTask for each tile

    Args:
        layer LayerConfig: layer configuration
        bbox Dict: bounding box with X_START, X_ENDE, Y_START, Y_ENDE keys

    Returns:
        List[DownloadTask]: list of download tasks
    """
    tasks = []
    save_dir = layer.subdir
    os.makedirs(save_dir, exist_ok=True)

    y = bbox["Y_START"]
    row_idx = 0
    while y > bbox["Y_ENDE"]:
        current_y_min = y - layer.kachel_hoehe_meter
        x = bbox["X_START"]
        col_idx = 0
        while x < bbox["X_ENDE"]:
            current_x_max = x + layer.kachel_breite_meter
            fname = f"z{row_idx:03d}_s{col_idx:03d}.png"
            fpath = os.path.join(save_dir, fname)
            pgw = erstelle_pgw_inhalt(x, current_y_min, current_x_max, y, layer.pixel_width, layer.pixel_height)
            bbox_str = f"{x},{current_y_min},{current_x_max},{y}"
            
            params = {}
            if layer.service_type == "wms":
                params = {
                    'SERVICE': 'WMS', 'VERSION': '1.1.1', 'REQUEST': 'GetMap',
                    'FORMAT': 'image/png', 'TRANSPARENT': 'true',
                    'LAYERS': layer.layers_param, 'STYLES': '', 'SRS': 'EPSG:3857', 
                    'WIDTH': str(layer.pixel_width), 'HEIGHT': str(layer.pixel_height), 'BBOX': bbox_str
                }
            else:
                params = {
                    'bbox': bbox_str, 'size': f"{layer.pixel_width},{layer.pixel_height}",
                    'dpi': layer.dpi, 'format': 'png32', 'transparent': 'true',
                    'bboxSR': layer.bboxSR, 'imageSR': layer.imageSR, 'layers': layer.layers_param, 'f': 'image'
                }

            # tile id for logging
            t_id = f"{layer.name}_{row_idx}_{col_idx}"
            tasks.append(DownloadTask(url=layer.base_url, params=params, filepath=fpath, pgw_content=pgw, tile_id=t_id))

            x = current_x_max
            col_idx += 1
        y = current_y_min
        row_idx += 1
    return tasks

async def run_async_download() -> None:
    """
    main async download function

    Returns:
        None
    """
    # setup logger
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger("DOWNLOADER")

    print(f"🚀 Starte Async-Downloader (High Performance)")
    print(f"   -> Konkurrenz: {MAX_CONCURRENT_REQUESTS} parallele Anfragen")

    # TCPConnector is optimizing connection pooling
    # TCPConnector with ssl context and no limit on connections (semaphore used instead)
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=0, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=60)  # general timeout for requests

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        # cache checking phase
        print(f"\n🔍 Prüfe Cache...")
        layers_to_download = []

        for layer in DOWNLOAD_LAYERS:
            cache_valid = await is_cache_valid(layer, session)
            if cache_valid:
                print(f"   ✅ {layer.name}: Cache gültig, überspringe Download")
            else:
                print(f"   ❌ {layer.name}: Cache ungültig oder nicht vorhanden, wird neu heruntergeladen")
                layers_to_download.append(layer)

        if not layers_to_download:
            print(f"\n✅ Alle Daten sind gecacht und gültig! Kein Download notwendig.")
            return

        print(f"\n⬇️  Starte Download für {len(layers_to_download)}/{len(DOWNLOAD_LAYERS)} Layer...\n")

        # download phase
        all_tasks = []
        for layer in layers_to_download:
            t = prepare_tasks(layer, ANALYSE_BBOX)
            all_tasks.extend(t)
            print(f"   -> {layer.name}: {len(t)} Kacheln vorbereitet.")

        # Semaphore is used to limit cocurrent requests
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # create download tasks
        tasks = [download_worker(session, task, sem) for task in all_tasks]

        # tqdm.gather shows progress bar for async tasks
        results = await tqdm.gather(*tasks, unit="img", colour="green", desc="Downloading")

        success_count = sum(results)
        print(f"✅ Download abgeschlossen: {success_count}/{len(all_tasks)} erfolgreich.")

        # save cache metadata for downloaded layers
        for layer in layers_to_download:
            save_cache_metadata(layer)
            logger.info(f"💾 Cache-Metadaten gespeichert für {layer.name}")

def main() -> None:
    """
    main function to run the async downloader

    Returns:
        None
    """
    if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR)

    start = time.time()

    # Windows specific event loop policy (needed for aiohttp on Windows)
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run_async_download())

    duration = time.time() - start
    print(f"⏱️  Dauer: {duration:.2f} Sekunden")

if __name__ == "__main__":
    main()