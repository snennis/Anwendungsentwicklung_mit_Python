"""
cleans and processes geometry layers for berlin.
"""
import multiprocessing
import shapely
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

def get_city_shape(city: str) -> gpd.GeoSeries or box:
    """
    loads the shape of berlins bbox using OSMnx

    Args:
        city (str): city name to load shape for (e.g. "Berlin, Germany")

    Returns:
        gpd.GeoSeries or box: city boundary shape or fallback bbox
    """
    print("    🏙️ Lade Berlin-Grenze (OSMnx)...")
    try:
        # load berlin
        gdf = ox.geocode_to_gdf(city)
        # reproject
        gdf = gdf.to_crs(CRS)
        # extract geometry
        return gdf.dissolve().geometry.iloc[0]
    except Exception as e:
        print(f"    ⚠️ Fehler beim Laden der Grenze: {e}")
        # Fallback BBox (Berlin UTM33)
        return box(360000, 5800000, 420000, 5860000)

def clean_geometry_layer(config: dict, boundary_shape: shapely.geometry) -> tuple[str or None, gpd.GeoDataFrame or None]:
    """
    worker function to clean a single geometry layer

    Args:
        config (dict): configuration for layer to clean
        boundary_shape (shapely.geometry): boundary shape to clip to (e.g. berlin)

    Returns:
        tuple[str or None, gpd.GeoDataFrame or None]: key and cleaned GeoDataFrame or None on failure
    """
    in_path = config["input"]
    out_path = config["output"]
    radius = config["radius"]
    
    if not os.path.exists(in_path):
        return None, None

    # get logger locally
    logging.info(f"Verarbeite {config['name']}...")
    
    try:
        # 1. load data
        gdf = gpd.read_file(in_path, engine="pyogrio")
        if gdf.empty:
            logging.warning(f"{config['name']} ist leer.")
            return None, None

        # 2. reproject
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)

        # 3. clip to boundary (shape of berlin)
        # we use spatial indexing first for speed
        try:
            xmin, ymin, xmax, ymax = boundary_shape.bounds
            gdf = gdf.cx[xmin:xmax, ymin:ymax]
            gdf = gdf.clip(boundary_shape)
        except Exception:
            # Fallback if clipping fails
            pass
        
        if gdf.empty:
            logging.info(f"{config['name']}: Nach Clipping leer.")
            return None, None

        # 4. PRE-CLEANING (Simplify & Make Valid)
        gdf['geometry'] = gdf.geometry.make_valid()
        gdf['geometry'] = gdf.geometry.simplify(tolerance=0.5, preserve_topology=True)

        # 5. BUFFER & UNION (Core Optimization)
        # step a: Positive Buffer
        buffered_geoms = gdf.geometry.buffer(radius, resolution=3)
        
        # step b: Merge all Geometries
        merged_geom = unary_union(buffered_geoms)
        
        # step c: Negative Buffer
        final_geom = merged_geom.buffer(-radius, resolution=3)

        # step d: Final Make Valid
        final_geom = final_geom.buffer(0)

        # 6. write to GeoDataFrame
        if final_geom.is_empty:
            logging.warning(f"{config['name']} wurde komplett weg-gefiltert.")
            return None, None

        out_gdf = gpd.GeoDataFrame({'geometry': [final_geom]}, crs=CRS)

        # Explode MultiPolygons to single Polygons
        out_gdf = out_gdf.explode(index_parts=False).reset_index(drop=True)

        logging.info(f"✅ Fertig: {config['name']} ({len(out_gdf)} Polygone)")

        # 8. return key and cleaned gdf for in-memory storage
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

def main() -> None:
    """
    main function to run cleaning in parallel

    Returns:
        None
    """
    if not os.path.exists(BASE_DIR): return
    # setup logging
    logging.basicConfig(level=logging.WARNING, format='%(message)s')

    print("🚀 Starte Geometrie-Cleaning (Optimiert + RAM)")
    
    # 1. load city shape for berlin
    berlin_shape = get_city_shape("Berlin, Germany")
    
    # 2. prepare worker function
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