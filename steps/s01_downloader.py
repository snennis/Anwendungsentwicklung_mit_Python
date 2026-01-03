import os
import requests
import logging
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from config import BASE_DIR, get_log_path, ANALYSE_BBOX, LayerConfig, DOWNLOAD_LAYERS, DOWNLOAD_MAX_WORKERS, dataclass

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
    # Logger holen (nutzt die Konfiguration aus main.py)
    logger = logging.getLogger("DOWNLOADER")
    
    try:
        # Request absetzen
        with get_session().get(task.url, params=task.params, stream=True, timeout=30) as r:
            
            # 1. Wirft Fehler bei 4xx oder 5xx Statuscodes
            r.raise_for_status()
            
            content = r.content
            
            # 2. Plausibilitäts-Check (Leere Bilder vom Server abfangen)
            if len(content) < 500:
                logger.warning(f"⚠️  Datei zu klein (<500b), wird ignoriert: {task.tile_id}")
                return False

            # 3. Schreiben auf die Festplatte (Separat abgesichert)
            try:
                with open(task.filepath, 'wb') as f: 
                    f.write(content)
                with open(task.filepath.replace(".png", ".pgw"), 'w') as f: 
                    f.write(task.pgw_content)
                return True
                
            except OSError as e:
                # Passiert z.B. wenn Disk voll ist oder Schreibrechte fehlen
                logger.error(f"💾 Schreibfehler bei {task.filepath}: {e}")
                return False

    except requests.exceptions.Timeout:
        # Wichtig bei Massen-Downloads: Timeouts erkennen
        logger.warning(f"⏳ Timeout (30s) bei Kachel {task.tile_id}")
        return False
        
    except requests.exceptions.ConnectionError:
        logger.error(f"🔌 Verbindungsfehler bei Kachel {task.tile_id} (Netzwerk weg?)")
        return False
        
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status == 404:
            # 404 ist bei Kacheln oft normal (Randgebiete), daher nur Info/Debug
            logger.info(f"Kachel nicht vorhanden (404): {task.tile_id}")
        elif status == 429:
             logger.critical(f"🛑 RATE LIMIT! Wir werden geblockt (429) bei {task.tile_id}")
             # Hier könnte man theoretisch ein time.sleep einbauen
        else:
            logger.error(f"❌ Server-Fehler {status} bei {task.tile_id}")
        return False
        
    except Exception as e:
        # Der "Catch-All" nur für wirklich unerwartete Dinge (z.B. MemoryError)
        logger.critical(f"🔥 Unbekannter Crash bei {task.tile_id}: {e}")
        return False

def prepare_tasks(layer: LayerConfig, bbox: Dict) -> List[DownloadTask]:
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
    if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR)
    # Logging is configured in main.py
    
    print("🚀 Starte Download-Phase...")
    print(f"  -> Max. Worker: {DOWNLOAD_MAX_WORKERS}")

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
