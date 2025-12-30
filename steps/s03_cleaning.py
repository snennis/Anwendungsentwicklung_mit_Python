import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
import logging
from shapely.geometry import box
from config import BASE_DIR, get_log_path, CRS, CLEANING_LAYERS

LOG_FILE = get_log_path("03_cleaning.log")

# Cache für die Grenze
_BERLIN_BOUNDARY_CACHE = None

def get_city_shape(city: str):
    """Lädt die exakte Grenze von Berlin (ohne Brandenburg)."""
    global _BERLIN_BOUNDARY_CACHE
    if _BERLIN_BOUNDARY_CACHE is not None:
        return _BERLIN_BOUNDARY_CACHE

    print("   🏙️ Lade Berlin-Grenze für Clipping...")
    try:
        # Lade Berlin
        gdf = ox.geocode_to_gdf(city)
        # Reprojizieren
        gdf = gdf.to_crs(CRS)
        # Dissolve (falls mehrere Teile)
        _BERLIN_BOUNDARY_CACHE = gdf.dissolve().geometry.iloc[0]
        return _BERLIN_BOUNDARY_CACHE
    except Exception as e:
        print(f"   ⚠️ Fehler beim Laden der Grenze: {e}")
        # Fallback BBox (Ungefähr Berlin Mitte)
        return box(360000, 5800000, 420000, 5860000)

def clean_geometry_layer(config):
    in_path = os.path.join(BASE_DIR, config["input"])
    out_path = os.path.join(BASE_DIR, config["output"])
    radius = config["radius"]
    
    if not os.path.exists(in_path):
        return

    print(f"🧹 Verarbeite {config['name']}...")
    
    try:
        # 1. Laden
        gdf = gpd.read_file(in_path)
        if gdf.empty:
            print(f"   ⚠️ Leer.")
            return

        # 2. Reprojektion
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)

        # 3. HARD CLIPPING (Alles außerhalb von Berlin abschneiden)
        berlin_shape = get_city_shape("Berlin, Germany")
        
        print(f"   ✂️ Schneide auf Stadtgrenze zu...")
        gdf = gdf.clip(berlin_shape)
        
        if gdf.empty:
            print(f"   ⚠️ Nach Clipping leer (keine Daten in Berlin).")
            return

        print(f"   🔧 Repariere Korridore (Radius: {radius}m)...")
        
        # 4. Cleaning (Buffer-Trick)
        gdf['geometry'] = gdf.geometry.buffer(radius, resolution=3)
        gdf = gdf.dissolve()
        gdf['geometry'] = gdf.geometry.buffer(-radius, resolution=3)
        gdf['geometry'] = gdf.geometry.buffer(0)
        
        # 5. Speichern
        gdf.to_file(out_path, driver="GPKG")
        print(f"   ✅ Fertig: {config['output']}")
        
    except Exception as e:
        logging.error(f"Fehler bei {config['name']}: {e}")
        print(f"   ❌ Fehler: {e}")

def main():
    if not os.path.exists(BASE_DIR): return
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_FILE, mode='w')])
    
    print("🚀 Starte Geometrie-Cleaning & Clipping")
    for layer in CLEANING_LAYERS:
        clean_geometry_layer(layer)
    print("\n✨ Fertig.")

if __name__ == "__main__":
    main()
