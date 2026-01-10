import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
import logging
from shapely.geometry import box
from shapely.ops import unary_union
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from config import BASE_DIR, CRS, CLEANING_LAYERS

def get_city_shape(city: str):
    """
    Lädt die exakte Grenze von Berlin via OSMnx.
    """
    print("    🏙️ Lade Berlin-Grenze (OSMnx)...")
    try:
        # Lade Berlin
        gdf = ox.geocode_to_gdf(city)
        # Reprojizieren
        gdf = gdf.to_crs(CRS)
        # Geometrie extrahieren
        return gdf.dissolve().geometry.iloc[0]
    except Exception as e:
        print(f"    ⚠️ Fehler beim Laden der Grenze: {e}")
        # Fallback BBox (Berlin UTM33)
        return box(360000, 5800000, 420000, 5860000)

def clean_geometry_layer(config, boundary_shape):
    """
    Worker-Funktion: Liest, clippt und bereinigt Geometrien.
    Optimiert mit unary_union statt dissolve und gibt Ergebnisse für RAM zurück.
    """
    in_path = config["input"]
    out_path = config["output"]
    radius = config["radius"]
    
    if not os.path.exists(in_path):
        return None, None

    # Logger lokal holen
    logging.info(f"Verarbeite {config['name']}...")
    
    try:
        # 1. Laden (Pyogrio für Speed)
        gdf = gpd.read_file(in_path, engine="pyogrio")
        if gdf.empty:
            logging.warning(f"{config['name']} ist leer.")
            return None, None

        # 2. Reprojektion
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)

        # 3. CLIP (Zuerst grob filtern spart Rechenzeit)
        # Wir nutzen Spatial Index für schnelles Filtern, dann echtes Clipping
        try:
            xmin, ymin, xmax, ymax = boundary_shape.bounds
            gdf = gdf.cx[xmin:xmax, ymin:ymax]
            gdf = gdf.clip(boundary_shape)
        except Exception:
            # Fallback falls Geometrien kaputt sind
            pass
        
        if gdf.empty:
            logging.info(f"{config['name']}: Nach Clipping leer.")
            return None, None

        # 4. PRE-CLEANING (Simplify & Make Valid)
        # Das ist der Schlüssel zur Geschwindigkeit!
        # Weniger Punkte = Schnellerer Buffer/Union.
        gdf['geometry'] = gdf.geometry.make_valid()
        gdf['geometry'] = gdf.geometry.simplify(tolerance=0.5, preserve_topology=True)

        # 5. BUFFER & UNION (Core Optimization)
        # Schritt A: Buffer auf Einzel-Geometrien
        buffered_geoms = gdf.geometry.buffer(radius, resolution=3)
        
        # Schritt B: Unary Union statt Dissolve (Viel schneller!)
        # Verschmilzt alles zu einem einzigen MultiPolygon
        merged_geom = unary_union(buffered_geoms)
        
        # Schritt C: Negative Buffer (Artefakte entfernen) auf dem Ergebnis
        final_geom = merged_geom.buffer(-radius, resolution=3)

        # Schritt D: Clean Up (Buffer 0)
        final_geom = final_geom.buffer(0)

        # 6. RE-KONSTRUKTION
        # Wir packen die Geometrie zurück in ein GeoDataFrame
        if final_geom.is_empty:
            logging.warning(f"{config['name']} wurde komplett weg-gefiltert.")
            return None, None

        out_gdf = gpd.GeoDataFrame({'geometry': [final_geom]}, crs=CRS)

        # Explode, damit wir nicht ein riesiges MultiPolygon haben, sondern sinnvolle Teile
        out_gdf = out_gdf.explode(index_parts=False).reset_index(drop=True)

        # 7. SPEICHERN (Backup auf Disk)
        # out_gdf.to_file(out_path, driver="GPKG", engine="pyogrio")
        logging.info(f"✅ Fertig: {config['name']} ({len(out_gdf)} Polygone)")

        # 8. RETURN FÜR RAM (Übergabe an s04)
        key_map = {
            "Telekom 2000": "tk_2000",
            "Telekom 1000": "tk_1000",
            "Telekom Plan": "tk_plan",
            "Vodafone 1000": "vf_1000"
        }
        return key_map.get(config['name']), out_gdf

    except Exception as e:
        logging.error(f"Fehler bei {config['name']}: {e}")
        return None, None

def main():
    """
    main function to run cleaning in parallel
    """
    if not os.path.exists(BASE_DIR): return
    # Einfaches Logging für Standalone-Run
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    print("🚀 Starte Geometrie-Cleaning (Optimiert + RAM)")
    
    # 1. Grenze EINMAL laden
    berlin_shape = get_city_shape("Berlin, Germany")
    
    # 2. Worker vorbereiten
    worker_func = partial(clean_geometry_layer, boundary_shape=berlin_shape)

    memory_buffer = {}

    # 3. Parallel ausführen
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(worker_func, CLEANING_LAYERS))

        # Ergebnisse einsammeln
        for key, gdf in results:
            if key and gdf is not None:
                memory_buffer[key] = gdf
    
    print(f"\n✨ Cleaning abgeschlossen. {len(memory_buffer)} Layer im RAM.")
    return memory_buffer

if __name__ == "__main__":
    main()