import os
import sys
import glob
import logging
from typing import List, Tuple
from tqdm import tqdm
import numpy as np

# --- ENVIRONMENT FIX (für PROJ/GDAL) ---
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

from config import BASE_DIR, get_log_path, ProcessConfig, ExtractionRule, PROCESSING_LAYERS

LOG_FILE = get_log_path("02_processing.log")

def process_single_file(filepath: str, rule: ExtractionRule) -> List[dict]:
    features = []
    try:
        with rasterio.open(filepath) as src:
            rgba = src.read((1,2,3,4))
            transform = src.transform
            target = rule.color_rgba
            
            # Maskierung
            mask = np.logical_and.reduce((
                rgba[0] == target[0], rgba[1] == target[1],
                rgba[2] == target[2], rgba[3] > 100
            ))
            
            # Pixel-Cleaning
            if SCIPY_AVAILABLE and np.sum(mask) > 0:
                mask = binary_closing(mask, structure=np.ones((3,3))).astype(mask.dtype)
            
            if np.sum(mask) > 0:
                shapes = rasterio_shapes(mask.astype(rasterio.uint8), mask=mask, transform=transform)
                for geom, val in shapes:
                    if val == 1:
                        features.append({'geometry': shapely_shape(geom), 'category': rule.name})
    except Exception:
        pass
    return features

def process_layer_stream(config: ProcessConfig):
    tile_dir = os.path.join(BASE_DIR, config.subdir)
    files = glob.glob(os.path.join(tile_dir, "*.png"))
    valid_files = [f for f in files if os.path.exists(f.replace(".png", ".pgw"))]
    
    print(f"⚙️ Verarbeite {config.name} ({len(valid_files)} Kacheln)...")
    
    for rule in config.rules:
        all_features = []
        for fp in tqdm(valid_files, desc=f"  -> {rule.name}", unit="tile", colour="blue"):
            all_features.extend(process_single_file(fp, rule))
            
        if all_features:
            out_path = os.path.join(BASE_DIR, rule.output_gpkg)
            gdf = geopandas.GeoDataFrame(all_features, crs="EPSG:3857")
            gdf.to_file(out_path, driver="GPKG", layer=rule.layer_name)
            print(f"     ✅ Gespeichert: {rule.output_gpkg} ({len(gdf)} Features)")
        else:
            print(f"     ℹ️ Leer: {rule.name}")

def main():
    if not os.path.exists(BASE_DIR): return
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_FILE, mode='w')])
    
    for layer in PROCESSING_LAYERS: process_layer_stream(layer)

if __name__ == "__main__":
    main()
