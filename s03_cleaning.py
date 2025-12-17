import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
import logging
from shapely.geometry import box
import config
import utils

# --- KONFIGURATION NEU ---
# Input -> Output Mapping basierend auf Config Keys
LAYERS_CONFIG = [
    {"input_key": "raw_tk_2000", "output_key": "clean_tk_2000", "radius": 7.0, "name": "Telekom 2000"},
    {"input_key": "raw_tk_1000", "output_key": "clean_tk_1000", "radius": 7.0, "name": "Telekom 1000"},
    {"input_key": "raw_tk_plan", "output_key": "clean_tk_plan", "radius": 7.0, "name": "Telekom Plan"},
    {"input_key": "raw_vf_1000", "output_key": "clean_vf_1000", "radius": 3.0, "name": "Vodafone 1000"}
]

def clean_geometry_layer(layer_cfg):
    in_path = config.GPKG_FILES[layer_cfg["input_key"]]
    out_path = config.GPKG_FILES[layer_cfg["output_key"]]
    radius = layer_cfg["radius"]
    
    logger = logging.getLogger("CLEANER")

    if not os.path.exists(in_path):
        return

    logger.info(f"🧹 Verarbeite {layer_cfg['name']}...")
    
    try:
        # 1. Laden
        gdf = gpd.read_file(in_path)
        if gdf.empty:
            logger.info(f"   ⚠️ Leer.")
            return

        # 2. Reprojektion
        if gdf.crs != config.ANALYSIS_CRS:
            gdf = gdf.to_crs(config.ANALYSIS_CRS)

        # 3. HARD CLIPPING (Alles außerhalb von Berlin abschneiden)
        # Nutze Shared Utility
        berlin_shape = utils.get_berlin_boundary()
        
        logger.info(f"   ✂️ Schneide auf Stadtgrenze zu...")
        gdf = gdf.clip(berlin_shape)
        
        if gdf.empty:
            logger.info(f"   ⚠️ Nach Clipping leer (keine Daten in Berlin).")
            return

        logger.info(f"   🔧 Repariere Korridore (Radius: {radius}m)...")
        
        # 4. Cleaning (Buffer-Trick)
        gdf['geometry'] = gdf.geometry.buffer(radius, resolution=3)
        gdf = gdf.dissolve()
        gdf['geometry'] = gdf.geometry.buffer(-radius, resolution=3)
        gdf['geometry'] = gdf.geometry.buffer(0)
        
        # 5. Speichern
        gdf.to_file(out_path, driver="GPKG")
        logger.info(f"   ✅ Fertig: {out_path}")
        
    except Exception as e:
        logger.exception(f"Fehler bei {layer_cfg['name']}: {e}")

def main():
    logger = utils.setup_logger("CLEANER", config.LOG_FILES["s03"])
    
    if not os.path.exists(config.HAUPTORDNER): return
    
    logger.info("🚀 Starte Geometrie-Cleaning & Clipping")
    for layer in LAYERS_CONFIG:
        clean_geometry_layer(layer)
    logger.info("✨ Fertig.")

if __name__ == "__main__":
    main()