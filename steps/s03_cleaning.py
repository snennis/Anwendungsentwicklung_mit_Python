import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
import logging
from shapely.geometry import box
from concurrent.futures import ProcessPoolExecutor
from functools import partial  # <--- NEU: Zum Fixieren von Argumenten
from config import BASE_DIR, CRS, CLEANING_LAYERS

def get_city_shape(city: str):
    """
    Lädt die exakte Grenze von Berlin.
    Kein Global State mehr. Einfach Input -> Output.
    """
    print("    🏙️ Lade Berlin-Grenze (Initial)...")
    try:
        # Lade Berlin
        gdf = ox.geocode_to_gdf(city)
        # Reprojizieren
        gdf = gdf.to_crs(CRS)
        # Dissolve & Geometrie extrahieren
        return gdf.dissolve().geometry.iloc[0]
    except Exception as e:
        print(f"    ⚠️ Fehler beim Laden der Grenze: {e}")
        # Fallback BBox
        return box(360000, 5800000, 420000, 5860000)

def clean_geometry_layer(config, boundary_shape):
    """
    Verarbeitet einen Layer. 
    WICHTIG: boundary_shape wird jetzt übergeben, nicht global geholt.
    """
    in_path = config["input"]
    out_path = config["output"]
    radius = config["radius"]
    
    if not os.path.exists(in_path):
        return

    # Logger holen (lokal im Prozess)
    logging.info(f"Verarbeite {config['name']}...")
    
    try:
        # 1. Laden
        gdf = gpd.read_file(in_path, engine="pyogrio")
        if gdf.empty:
            logging.warning(f"{config['name']} ist leer.")
            return

        # 2. Reprojektion
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)

        # 3. HARD CLIPPING (Mit übergebener Shape)
        # Wir nutzen hier intersects vor dem Clip für Performance, falls möglich,
        # aber clip() reicht meistens.
        gdf = gdf.clip(boundary_shape)
        
        if gdf.empty:
            logging.info(f"{config['name']}: Nach Clipping leer.")
            return

        # 4. Cleaning (Buffer-Trick)
        # Fix invalid geometries first
        gdf['geometry'] = gdf.geometry.make_valid()
        
        gdf['geometry'] = gdf.geometry.buffer(radius, resolution=3)
        gdf = gdf.dissolve()
        gdf['geometry'] = gdf.geometry.buffer(-radius, resolution=3)
        gdf['geometry'] = gdf.geometry.buffer(0) # Letzter Clean-Up
        
        # 5. Speichern
        gdf.to_file(out_path, driver="GPKG", engine="pyogrio")
        logging.info(f"✅ Fertig: {config['name']}")
        
    except Exception as e:
        logging.error(f"Fehler bei {config['name']}: {e}")

def main():
    if not os.path.exists(BASE_DIR): return
    # Logging Config aus main.py greift hier, wenn als Modul geladen.
    # Wenn standalone ausgeführt, kurzes Basic Config:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    print("🚀 Starte Geometrie-Cleaning & Clipping")
    
    # 1. Grenze EINMAL laden (im Hauptprozess)
    berlin_shape = get_city_shape("Berlin, Germany")
    
    # 2. Funktion "vorbereiten" (Currying)
    # Wir erstellen eine neue Funktion, bei der 'boundary_shape' schon ausgefüllt ist.
    # Der Worker muss dann nur noch 'config' liefern.
    worker_func = partial(clean_geometry_layer, boundary_shape=berlin_shape)
    
    # 3. Parallel ausführen
    with ProcessPoolExecutor() as executor:
        # map übergibt nun jedes Element aus CLEANING_LAYERS als erstes 
        # freies Argument an worker_func (das ist 'config')
        list(executor.map(worker_func, CLEANING_LAYERS))
    
    print("\n✨ Fertig.")

if __name__ == "__main__":
    main()