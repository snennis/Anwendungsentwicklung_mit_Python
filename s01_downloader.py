import os
import requests
import logging
from typing import Dict, Any, List
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# --- KONFIGURATION ---
ANALYSE_BBOX = {
   "X_START": 1250000.0,
   "Y_START": 7080000.0,
   "X_ENDE": 1660000.0,
   "Y_ENDE": 6750000.0
}

HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "download.log")
MAX_WORKERS = 20

@dataclass
class LayerConfig:
    name: str
    service_type: str
    base_url: str
    layers_param: str
    kachel_breite_meter: float
    kachel_hoehe_meter: float
    pixel_width: int
    pixel_height: int
    subdir: str
    dpi: float = 96
    bboxSR: str = "3857"
    imageSR: str = "3857"

@dataclass
class DownloadTask:
    url: str
    params: Dict[str, Any]
    filepath: str
    pgw_content: str
    tile_id: str

def erstelle_pgw_inhalt(xmin, ymin, xmax, ymax, w_px, h_px):
    A = (xmax - xmin) / float(w_px)
    E = -(ymax - ymin) / float(h_px)
    C = xmin + (A / 2.0)
    F = ymax + (E / 2.0)
    return f"{A:.10f}\n0.0\n0.0\n{E:.10f}\n{C:.10f}\n{F:.10f}\n"

def get_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount('https://', adapter)
    return s

def download_worker(task: DownloadTask) -> bool:
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
    except:
        pass
    return False

def prepare_tasks(layer: LayerConfig, bbox: Dict, output_base: str) -> List[DownloadTask]:
    tasks = []
    save_dir = os.path.join(output_base, layer.subdir)
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
    if not os.path.exists(HAUPTORDNER): os.makedirs(HAUPTORDNER)
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_DATEINAME, mode='w')])
    
    print("🚀 Starte Download-Phase...")

    # Grid Parameter (Optimiert)
    tk_res = (1488381.81 - 1487158.82) / 256.0
    tk_dim = 2048 * tk_res
    
    vf_res = (1489909.16 - 1487728.32) / 1506.0
    vf_w = 3000 * vf_res
    vf_h = int(3000 * (1793/1506)) * vf_res

    CONFIGS = [
        LayerConfig("Telekom_Fiber_Total", "wms", "https://t-map.telekom.de/tmap2/geoserver/public/tmap/public/wms", 
                   "public:coverage_fixedline_fiber", tk_dim, tk_dim, 2048, 2048, "tiles_tk_fiber"),
        LayerConfig("Vodafone_Fiber_Total", "arcgis", "https://netmap.vodafone.de/arcgis/rest/services/CoKart/netzabdeckung_fixnet_4x/MapServer/export", 
                   "show:3", vf_w, vf_h, 3000, int(3000 * (1793/1506)), "tiles_vf_fiber", dpi=158.4, bboxSR="102100", imageSR="102100")
    ]

    all_tasks = []
    for layer in CONFIGS:
        tasks = prepare_tasks(layer, ANALYSE_BBOX, HAUPTORDNER)
        all_tasks.extend(tasks)
        print(f"  -> {layer.name}: {len(tasks)} Kacheln.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(download_worker, all_tasks), total=len(all_tasks), unit="img", colour="green"))

    print("✅ Download abgeschlossen.")

if __name__ == "__main__":
    main()