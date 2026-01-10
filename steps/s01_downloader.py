import os
import asyncio
import aiohttp
import aiofiles
import logging
import time
from typing import Dict, Any, List
from tqdm.asyncio import tqdm
from config import BASE_DIR, ANALYSE_BBOX, LayerConfig, DOWNLOAD_LAYERS, dataclass

# --- KONFIGURATION ---
# Wie viele Anfragen gleichzeitig?
# Zu hoch = Ban Gefahr! 50 ist aggressiv, aber meist okay.
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
    Asynchroner Worker. Nutzt Semaphore um die Anzahl gleichzeitiger Requests zu begrenzen.
    """
    logger = logging.getLogger("DOWNLOADER")

    # Semaphore blockiert, wenn zu viele Anfragen gleichzeitig laufen
    async with semaphore:
        try:
            async with session.get(task.url, params=task.params, timeout=30) as response:

                # 1. Status Check
                if response.status != 200:
                    if response.status == 404:
                        logger.debug(f"Kachel nicht gefunden (404): {task.tile_id}")
                    elif response.status == 429:
                        logger.critical(f"🛑 RATE LIMIT (429) bei {task.tile_id}! Wir sind zu schnell.")
                    else:
                        logger.error(f"❌ HTTP {response.status} bei {task.tile_id}")
                    return False

                # 2. Content lesen
                content = await response.read()

                # 3. Plausibilitäts-Check (Leere Bilder ignorieren)
                if len(content) < 500:
                    logger.warning(f"⚠️ Datei zu klein (<500b), ignoriere: {task.tile_id}")
                    return False

                # 4. Asynchrones Schreiben (Non-blocking I/O)
                try:
                    async with aiofiles.open(task.filepath, 'wb') as f:
                        await f.write(content)

                    # PGW Datei (ist winzig, kann auch blockierend geschrieben werden, aber sauberer so)
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

    Args:
        layer LayerConfig: layer configuration
        bbox Dict: bounding box with X_START, X_ENDE, Y_START, Y_ENDE keys

    Returns:
        List[DownloadTask]: list of download tasks
    """
    tasks = []
    save_dir = layer.subdir
    if not os.path.exists(save_dir): os.makedirs(save_dir)
    
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

            # Tile ID für Logs
            t_id = f"{layer.name}_{row_idx}_{col_idx}"
            tasks.append(DownloadTask(url=layer.base_url, params=params, filepath=fpath, pgw_content=pgw, tile_id=t_id))

            x = current_x_max
            col_idx += 1
        y = current_y_min
        row_idx += 1
    return tasks

async def run_async_download():
    # Logging Setup für Standalone-Test
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

    print(f"🚀 Starte Async-Downloader (High Performance)")
    print(f"   -> Konkurrenz: {MAX_CONCURRENT_REQUESTS} parallele Anfragen")

    all_tasks = []
    for layer in DOWNLOAD_LAYERS:
        t = prepare_tasks(layer, ANALYSE_BBOX)
        all_tasks.extend(t)
        print(f"   -> {layer.name}: {len(t)} Kacheln vorbereitet.")

    # Semaphore begrenzt die maximale Anzahl gleichzeitiger Verbindungen
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # TCPConnector optimiert connection pooling
    # limit=0 bedeutet keine harte Grenze im Connector, wir regeln das über Semaphore
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)

    timeout = aiohttp.ClientTimeout(total=60) # Generelles Timeout

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [download_worker(session, task, sem) for task in all_tasks]

        # tqdm.gather zeigt den Fortschrittsbalken asynchron an
        results = await tqdm.gather(*tasks, unit="img", colour="green", desc="Downloading")

    success_count = sum(results)
    print(f"✅ Download abgeschlossen: {success_count}/{len(all_tasks)} erfolgreich.")

def main():
    if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR)

    start = time.time()

    # Windows Selector Event Loop Policy Fix (wichtig für Python 3.8+ auf Windows)
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run_async_download())

    duration = time.time() - start
    print(f"⏱️  Dauer: {duration:.2f} Sekunden")

if __name__ == "__main__":
    main()