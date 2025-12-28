import os
import sys
import glob
import logging
import shutil
from typing import List, Tuple, Dict
from dataclasses import dataclass
from tqdm import tqdm
import numpy as np
from multiprocessing import Pool, cpu_count

# --- ENVIRONMENT FIX ---
try:
    base_prefix = sys.prefix
    possible_proj_paths = [
        os.path.join(base_prefix, 'Library', 'share', 'proj'),
        os.path.join(base_prefix, 'share', 'proj'),
        os.path.join(base_prefix, 'lib', 'site-packages', 'rasterio', 'proj_data'),
        os.path.join(base_prefix, 'Lib', 'site-packages', 'rasterio', 'proj_data')
    ]
    for p in possible_proj_paths:
        if os.path.exists(p):
            os.environ['PROJ_LIB'] = p
            break
except Exception:
    pass

import rasterio
from rasterio.features import shapes as rasterio_shapes
import geopandas
from shapely.geometry import shape as shapely_shape

try:
    from scipy.ndimage import binary_closing
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

HAUPTORDNER = "Glasfaser_Analyse_Project"
OUTPUT_ORDNER = os.path.join(HAUPTORDNER, "output_parquet") # Neu: Separater Output Ordner
LOG_DATEINAME = os.path.join(HAUPTORDNER, "02_processing.log")

@dataclass
class ExtractionRule:
    name: str
    color_rgba: Tuple[int, int, int, int]
    output_subdir: str # Geändert: Subdir statt Dateiname
    layer_name: str

@dataclass
class ProcessConfig:
    name: str
    subdir: str
    rules: List[ExtractionRule]

def hex_to_rgba(hex_code: str) -> Tuple[int, int, int, int]:
    hex_code = hex_code.lstrip('#')
    rgb = tuple(int(hex_code[i:i + 2], 16) for i in (0, 2, 4))
    return (*rgb, 255)

def process_single_file(args):
    """
    Verarbeitet eine einzelne Kachel.
    Keine Änderung an der Logik, nur robustes Error-Handling.
    """
    filepath: str
    rules: List[ExtractionRule]
    filepath, rules = args
    results = {rule.name: [] for rule in rules}

    try:
        with rasterio.open(filepath) as src:
            rgba = src.read((1, 2, 3, 4))
            transform = src.transform

            for rule in rules:
                target = rule.color_rgba
                # Maskierung
                mask = np.logical_and.reduce((
                    rgba[0] == target[0], rgba[1] == target[1],
                    rgba[2] == target[2], rgba[3] > 100
                ))

                if SCIPY_AVAILABLE and np.sum(mask) > 0:
                    mask = binary_closing(mask, structure=np.ones((3, 3))).astype(mask.dtype)

                if np.sum(mask) > 0:
                    shapes = rasterio_shapes(mask.astype(rasterio.uint8), mask=mask, transform=transform)
                    for geom, val in shapes:
                        if val == 1:
                            # Wir geben hier das nackte Dict zurück, um Overhead zu sparen?
                            # Nein, Shapely Objects sind okay, aber kosten beim Pickling.
                            # Für max Performance könnte man hier WKB (bytes) zurückgeben.
                            # Wir bleiben bei Shapely für Lesbarkeit, da IO der Bottleneck ist.
                            results[rule.name].append({'geometry': shapely_shape(geom), 'category': rule.name})
    except Exception as e:
        # Logging im Worker ist schwierig, hier ignorieren wir Fehler
        pass
    return results

def flush_buffer_to_parquet(buffer: List[Dict], output_path: str, part_id: int):
    """Schreibt den aktuellen Buffer in eine Parquet-Partition."""
    if not buffer:
        return
    
    filename = f"part_{part_id:05d}.parquet"
    full_path = os.path.join(output_path, filename)
    
    gdf = geopandas.GeoDataFrame(buffer, crs="EPSG:3857")
    # GeoParquet Speicherung: Schnell und komprimiert
    gdf.to_parquet(full_path, index=False, compression='snappy')

def process_layer_stream(config: ProcessConfig):
    tile_dir = os.path.join(HAUPTORDNER, config.subdir)
    files = glob.glob(os.path.join(tile_dir, "*.png"))
    valid_files = [f for f in files if os.path.exists(f.replace(".png", ".pgw"))]

    if not valid_files:
        print(f"⚠️ Keine Dateien für {config.name} gefunden.")
        return

    print(f"⚙️ Verarbeite {config.name} ({len(valid_files)} Kacheln)...")

    # Buffer Init
    # Struktur: { '2000 Mbit': [], '1000 Mbit': [] }
    buffers = {rule.name: [] for rule in config.rules}
    
    # Pfade vorbereiten
    for rule in config.rules:
        rule_path = os.path.join(OUTPUT_ORDNER, rule.output_subdir)
        os.makedirs(rule_path, exist_ok=True)

    BATCH_SIZE = 5000  # Anzahl Features, ab der geschrieben wird
    part_counters = {rule.name: 0 for rule in config.rules}
    
    num_workers = max(1, cpu_count() - 1)
    
    with Pool(num_workers) as pool:
        # imap_unordered ist der Schlüssel! Es liefert Ergebnisse, sobald sie da sind.
        iterator = pool.imap_unordered(process_single_file, [(f, config.rules) for f in valid_files], chunksize=10)
        
        for result_dict in tqdm(iterator, total=len(valid_files), desc="  Streaming & Writing", unit="tile"):
            
            # Ergebnisse in Buffer schieben
            for rule_name, features in result_dict.items():
                if features:
                    buffers[rule_name].extend(features)
            
            # Checken ob Buffer voll sind -> Schreiben
            for rule in config.rules:
                if len(buffers[rule.name]) >= BATCH_SIZE:
                    out_dir = os.path.join(OUTPUT_ORDNER, rule.output_subdir)
                    flush_buffer_to_parquet(buffers[rule.name], out_dir, part_counters[rule.name])
                    
                    buffers[rule.name] = [] # Reset Buffer
                    part_counters[rule.name] += 1

    # Reste schreiben (WICHTIG!)
    print("  🧹 Schreibe restliche Daten...")
    for rule in config.rules:
        if buffers[rule.name]:
            out_dir = os.path.join(OUTPUT_ORDNER, rule.output_subdir)
            flush_buffer_to_parquet(buffers[rule.name], out_dir, part_counters[rule.name])
            print(f"     ✅ {rule.name}: Fertig ({part_counters[rule.name] + 1} Parts)")

def main():
    if not os.path.exists(HAUPTORDNER): 
        print(f"Ordner {HAUPTORDNER} fehlt.")
        return
    
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_DATEINAME, mode='w')])

    # Output Ordner bereinigen (optional, Vorsicht!)
    # if os.path.exists(OUTPUT_ORDNER): shutil.rmtree(OUTPUT_ORDNER)

    # Config angepasst für Ordner-Struktur statt Dateinamen
    LAYERS = [
        ProcessConfig("Telekom Fiber", "tiles_tk_fiber", [
            ExtractionRule("2000 Mbit", hex_to_rgba("#610332"), "tk_2000", "2000"),
            ExtractionRule("1000 Mbit", hex_to_rgba("#7D4443"), "tk_1000", "1000"),
            ExtractionRule("Geplant", hex_to_rgba("#314EA5"), "tk_plan", "Plan"),
        ]),
        ProcessConfig("Vodafone Fiber", "tiles_vf_fiber", [
            ExtractionRule("1000 Mbit", hex_to_rgba("#7F0000"), "vf_1000", "1000")
        ])
    ]
    
    for layer in LAYERS: 
        process_layer_stream(layer)
        
    print(f"\n🚀 Fertig! Daten liegen als GeoParquet in: {OUTPUT_ORDNER}")
    print("ℹ️  Tipp: Du kannst in QGIS einfach den Ordner reinziehen oder DuckDB nutzen.")

if __name__ == "__main__":
    main()