"""
processes the downloaded map tiles into vector data based on predefined color rules.
saves the extracted features into a geopackage format.
"""
import os
import sys
import glob
import logging
from typing import List, Tuple
from dataclasses import dataclass
from tqdm import tqdm
import numpy as np
from concurrent.futures import ProcessPoolExecutor

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

from config import BASE_DIR, get_log_path, ProcessConfig, ExtractionRule, PROCESSING_LAYERS, DOWNLOAD_MAX_WORKERS

def process_single_file_wrapper(args) -> List[dict]:
    """
    wrapper function to unpack arguments for multiprocessing

    Args:
        args (tuple): tuple containing filepath (str) and rule (ExtractionRule)

    Returns:
        List[dict]: list of extracted features
    """
    return process_single_file(*args)

def process_single_file(filepath: str, rule: ExtractionRule) -> List[dict]:
    """
    processes a single raster file to extract features based on the given rule

    Args:
        filepath (str): path to the raster file
        rule (ExtractionRule): extraction rule containing color and category info

    Returns:
        List[dict]: list of extracted features
    """
    features = []
    try:
        with rasterio.open(filepath) as src:
            rgba = src.read((1,2,3,4))
            transform = src.transform
            target = rule.color_rgba
            
            # mask for pixels matching the target color with alpha > 100
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

def process_layer_stream(config: ProcessConfig) -> None:
    """
    processes all tiles in a given layer configuration

    Args:
        config (ProcessConfig): configuration for the processing layer

    Returns:
        None
    """
    tile_dir = config.subdir # Full path
    files = glob.glob(os.path.join(tile_dir, "*.png"))
    valid_files = [f for f in files if os.path.exists(f.replace(".png", ".pgw"))]
    
    print(f"⚙️ Verarbeite {config.name} ({len(valid_files)} Kacheln)...")
    
    for rule in config.rules:
        all_features = []

        # use all available CPU cores
        max_workers = DOWNLOAD_MAX_WORKERS
        
        task_args = [(f, rule) for f in valid_files]
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # map returns results in order
            results = list(tqdm(
                executor.map(process_single_file_wrapper, task_args), 
                total=len(valid_files), 
                desc=f"  -> {rule.name}", 
                unit="tile", 
                colour="blue"
            ))
            
        # Flatten results
        for res in results:
            all_features.extend(res)
            
        if all_features:
            out_path = rule.output_gpkg # Full path
            gdf = geopandas.GeoDataFrame(all_features, crs="EPSG:3857")
            gdf.to_file(out_path, driver="GPKG", layer=rule.layer_name, engine="pyogrio")
            print(f"     ✅ Gespeichert: {os.path.basename(out_path)} ({len(gdf)} Features)")
        else:
            print(f"     ℹ️ Leer: {rule.name}")

def main() -> None:
    """
    main function to process all layers as per config

    Returns:
        None
    """
    if not os.path.exists(BASE_DIR): return
    # Logging configured in main.py
    
    for layer in PROCESSING_LAYERS: process_layer_stream(layer)

if __name__ == "__main__":
    main()
