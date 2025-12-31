import os
import requests
import logging
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from dataclasses import dataclass
from config import BASE_DIR, get_log_path, ANALYSE_BBOX, LayerConfig, DOWNLOAD_LAYERS, DOWNLOAD_MAX_WORKERS

# Log file name (derived from config logic)
LOG_FILE = get_log_path("download.log")

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

def get_session() -> requests.Session:
    """
    creates a requests session with retry logic

    Args:
        none

    Returns:
        requests.Session: session object
    """
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount('https://', adapter)
    return s

def download_worker(task: DownloadTask) -> bool:
    """
    downloads a single tile based on the DownloadTask

    Args:
        task DownloadTask: task object with url, params, filepath, pgw_content, tile

    Returns:
        bool: true if download was successful, false otherwise
    """
    if os.path.exists(task.filepath):
        return True 
    try:
        with get_session().get(task.url, params=task.params, stream=True, timeout=30) as r:
            if r.status_code == 200:
                content = r.content
                if len(content) > 500:
                    with open(task.filepath, 'wb') as f: f.write(content)
                    with open(task.filepath.replace(".png", ".pgw"), 'w') as f: f.write(task.pgw_content)
                    return True
    except Exception as e:
        logging.warning(f"Fehler beim Download der Kachel {task.tile_id}")
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
    save_dir = layer.subdir # Is now full path
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
            tasks.append(DownloadTask(url=layer.base_url, params=params, filepath=fpath, pgw_content=pgw, tile_id=f"{row_idx}_{col_idx}"))
            x = current_x_max
            col_idx += 1
        y = current_y_min
        row_idx += 1
    return tasks

def main():
    """
    main function to execute the download phase
    """
    if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR)
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_FILE, mode='w')])
    
    print("🚀 Starte Download-Phase...")

    all_tasks = []
    for layer in DOWNLOAD_LAYERS:
        tasks = prepare_tasks(layer, ANALYSE_BBOX)
        all_tasks.extend(tasks)
        print(f"  -> {layer.name}: {len(tasks)} Kacheln.")

    with ThreadPoolExecutor(max_workers=DOWNLOAD_MAX_WORKERS) as executor:
        list(tqdm(executor.map(download_worker, all_tasks), total=len(all_tasks), unit="img", colour="green"))

    print("✅ Download abgeschlossen.")

if __name__ == "__main__":
    main()
