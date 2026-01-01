import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
from shapely.geometry import box
import logging
from config import BASE_DIR, get_log_path, CRS, ANALYSIS_INPUT_FILES, ANALYSIS_OUTPUT_GPKG

OUTPUT_GPKG = ANALYSIS_OUTPUT_GPKG
LOG_FILE = get_log_path("04_analysis.log")

def setup_logging():
    """
    setup logging config
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s',
                        handlers=[logging.FileHandler(LOG_FILE, mode='w'), logging.StreamHandler()])

def load_clean_layer(key: str) -> gpd.GeoDataFrame:
    """
    loads a cleaned layer from file

    Args:
        key (str): key of the layer in ANALYSIS_INPUT_FILES

    Returns:
        gpd.GeoDataFrame: loaded geodataframe
    """
    filepath = ANALYSIS_INPUT_FILES[key]
    if not os.path.exists(filepath):
        return gpd.GeoDataFrame(columns=['geometry', 'category'], crs=CRS)
    
    logging.info(f"Lade {key}...")
    gdf = gpd.read_file(filepath, engine="pyogrio")
    if gdf.crs != CRS:
        gdf = gdf.to_crs(CRS)
    return gdf

def get_boundary(city: str):
    """
    gets the boundary of a city, with OSM fallback to BBox

    Args:
        city (str): city name for geocoding

    Returns:
        gpd.GeoDataFrame: city boundary geodataframe
    """
    logging.info("Lade Stadtgrenze Berlin...")
    try:
        gdf_berlin = ox.geocode_to_gdf(city)
        return gdf_berlin.to_crs(CRS).dissolve()
    except:
        logging.warning("OSM Fehler. Nutze BBox Fallback.")
        bbox = box(360000, 5800000, 420000, 5860000) 
        return gpd.GeoDataFrame({'geometry': [bbox]}, crs=CRS)

def calculate_area_km2(gdf) -> float:
    """
    calculates area in km² of a geodataframe

    Args:
        gdf (gpd.GeoDataFrame): input geodataframe

    Returns:
        float: area in km²
    """
    if gdf.empty: return 0.0
    return gdf.geometry.area.sum() / 1_000_000

def main():
    """
    main analysis function
    """
    setup_logging()
    logging.info("🚀 Starte Analyse (Fokus: Berlin)")

    # 1. LADEN
    gdf_tk_2000 = load_clean_layer("tk_2000")
    gdf_tk_1000 = load_clean_layer("tk_1000")
    gdf_tk_plan = load_clean_layer("tk_plan")
    gdf_vf_1000 = load_clean_layer("vf_1000")

    # 2. AGGREGATION
    logging.info("Verschmelze Telekom Bestand...")
    gdf_tk_total = pd.concat([gdf_tk_2000, gdf_tk_1000])
    if not gdf_tk_total.empty: gdf_tk_total = gdf_tk_total.dissolve()

    # 3. MARKTSITUATION
    logging.info("Analysiere Wettbewerb...")
    gdf_competition = gpd.GeoDataFrame(columns=['geometry'], crs=CRS)
    gdf_monopol_tk = gdf_tk_total.copy()
    gdf_monopol_vf = gdf_vf_1000.copy()

    if not gdf_tk_total.empty and not gdf_vf_1000.empty:
        gdf_competition = gpd.overlay(gdf_tk_total, gdf_vf_1000, how='intersection')
        gdf_monopol_tk = gpd.overlay(gdf_tk_total, gdf_vf_1000, how='difference')
        gdf_monopol_vf = gpd.overlay(gdf_vf_1000, gdf_tk_total, how='difference')

    gdf_competition['status'] = 'Wettbewerb'
    gdf_competition['type'] = 'Bestand'
    gdf_monopol_tk['status'] = 'Monopol Telekom'
    gdf_monopol_tk['type'] = 'Bestand'
    gdf_monopol_vf['status'] = 'Monopol Vodafone'
    gdf_monopol_vf['type'] = 'Bestand'

    # 4. WHITE SPOTS (Referenz ist jetzt NUR Berlin)
    logging.info("Suche White Spots in Berlin...")
    boundary = get_boundary("Berlin, Germany")
    
    all_infra = pd.concat([gdf_tk_total, gdf_tk_plan, gdf_vf_1000])
    if not all_infra.empty:
        gdf_white = gpd.overlay(boundary, all_infra.dissolve(), how='difference')
    else:
        gdf_white = boundary # Berlin komplett ohne Netz
        
    gdf_white['status'] = 'White Spot'
    gdf_white['type'] = 'Lücke'

    if not gdf_tk_plan.empty:
        gdf_tk_plan['status'] = 'Telekom Planung'
        gdf_tk_plan['type'] = 'Planung'

    # 5. MERGE & STATS
    gdf_final = pd.concat([
        gdf_competition, gdf_monopol_tk, gdf_monopol_vf, gdf_white, gdf_tk_plan
    ], ignore_index=True)
    
    # Bereinigen leerer Geometrien (wichtig nach Overlay!)
    gdf_final = gdf_final[~gdf_final.is_empty & gdf_final.geometry.notna()]

    cols = ['geometry', 'status', 'type']
    gdf_final = gdf_final[[c for c in cols if c in gdf_final.columns]]

    print("\n" + "="*30)
    print("📊 STATISTIK BERLIN (km²)")
    print("="*30)
    stats = gdf_final.dissolve(by='status').area / 1_000_000
    print(stats.round(2))
    print("="*30 + "\n")

    if os.path.exists(OUTPUT_GPKG): os.remove(OUTPUT_GPKG)
    gdf_final.to_file(OUTPUT_GPKG, layer="analyse_berlin", driver="GPKG", engine="pyogrio")
    logging.info("✅ Fertig.")

if __name__ == "__main__":
    main()
