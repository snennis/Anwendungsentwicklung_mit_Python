import os
import sys
import glob
import logging
from typing import List, Tuple
from dataclasses import dataclass
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

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "02_processing.log")

@dataclass
class ExtractionRule:
    """
    rule for extracting feats from raster
    """
    name: str
    color_rgba: Tuple[int, int, int, int]
    output_gpkg: str
    layer_name: str

@dataclass
class ProcessConfig:
    """
    config for processing a layer
    """
    name: str
    subdir: str
    rules: List[ExtractionRule]

def hex_to_rgba(hex_code: str) -> Tuple[int, int, int, int]:
    """
    converts hex color code to rgba tuple

    Args:
        hex_code (str): hex color code

    Returns:
        Tuple[int, int, int, int]: rgba color tuple
    """
    hex_code = hex_code.lstrip('#')
    rgb = tuple(int(hex_code[i:i+2], 16) for i in (0, 2, 4))
    return (*rgb, 255)

def process_single_file(filepath: str, rule: ExtractionRule) -> List[dict]:
    """
    processes a single raster file and extracts feats based on rule

    Args:
        filepath (str): path to raster file
        rule (ExtractionRule): extraction rule

    Returns:
        List[dict]: list of extracted feats
    """
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
            
            # Pixel-Cleaning (Schließt nur winzige Pixel-Artefakte, keine 12m Korridore)
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
    processes a layer based on given config

    Args:
        config (ProcessConfig): processing config

    Returns:
        None
    """
    tile_dir = os.path.join(HAUPTORDNER, config.subdir)
    files = glob.glob(os.path.join(tile_dir, "*.png"))
    valid_files = [f for f in files if os.path.exists(f.replace(".png", ".pgw"))]
    
    print(f"⚙️ Verarbeite {config.name} ({len(valid_files)} Kacheln)...")
    
    for rule in config.rules:
        all_features = []
        for fp in tqdm(valid_files, desc=f"  -> {rule.name}", unit="tile", colour="blue"):
            all_features.extend(process_single_file(fp, rule))
            
        if all_features:
            out_path = os.path.join(HAUPTORDNER, rule.output_gpkg)
            # Speichert als "raw_" um Verwechslung zu vermeiden, oder direkt der Name
            # Wir behalten den Namen, Skript 03 wird ihn als Input nutzen.
            gdf = geopandas.GeoDataFrame(all_features, crs="EPSG:3857")
            gdf.to_file(out_path, driver="GPKG", layer=rule.layer_name)
            print(f"     ✅ Gespeichert: {rule.output_gpkg} ({len(gdf)} Features)")
        else:
            print(f"     ℹ️ Leer: {rule.name}")

def main():
    """
    main processing function
    """
    if not os.path.exists(HAUPTORDNER): return
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_DATEINAME, mode='w')])
    
    LAYERS = [
        ProcessConfig("Telekom Fiber", "tiles_tk_fiber", [
            ExtractionRule("2000 Mbit", hex_to_rgba("#610332"), "raw_tk_2000.gpkg", "2000"),
            ExtractionRule("1000 Mbit", hex_to_rgba("#7D4443"), "raw_tk_1000.gpkg", "1000"),
            ExtractionRule("Geplant", hex_to_rgba("#314EA5"), "raw_tk_plan.gpkg", "Plan"),
        ]),
        ProcessConfig("Vodafone Fiber", "tiles_vf_fiber", [
            ExtractionRule("1000 Mbit", hex_to_rgba("#7F0000"), "raw_vf_1000.gpkg", "1000")
        ])
    ]
    for layer in LAYERS: process_layer_stream(layer)

if __name__ == "__main__":
    main()