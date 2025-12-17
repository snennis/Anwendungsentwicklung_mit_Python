import os
import geopandas as gpd
import pandas as pd
import logging
import config
import utils

def load_clean_layer(key: str) -> gpd.GeoDataFrame:
    filepath = config.GPKG_FILES[key]
    logger = logging.getLogger("ANALYSIS")
    
    if not os.path.exists(filepath):
        return gpd.GeoDataFrame(columns=['geometry', 'category'], crs=config.ANALYSIS_CRS)
    
    logger.info(f"Lade {key}...")
    gdf = gpd.read_file(filepath)
    if gdf.crs != config.ANALYSIS_CRS:
        gdf = gdf.to_crs(config.ANALYSIS_CRS)
    return gdf

def calculate_area_km2(gdf):
    if gdf.empty: return 0.0
    return gdf.geometry.area.sum() / 1_000_000

def main():
    logger = utils.setup_logger("ANALYSIS", config.LOG_FILES["s04"])
    logger.info("🚀 Starte Analyse (Fokus: Berlin)")

    # 1. LADEN
    gdf_tk_2000 = load_clean_layer("clean_tk_2000")
    gdf_tk_1000 = load_clean_layer("clean_tk_1000")
    gdf_tk_plan = load_clean_layer("clean_tk_plan")
    gdf_vf_1000 = load_clean_layer("clean_vf_1000")

    # 2. AGGREGATION
    logger.info("Verschmelze Telekom Bestand...")
    gdf_tk_total = pd.concat([gdf_tk_2000, gdf_tk_1000])
    if not gdf_tk_total.empty: gdf_tk_total = gdf_tk_total.dissolve()

    # 3. MARKTSITUATION
    logger.info("Analysiere Wettbewerb...")
    gdf_competition = gpd.GeoDataFrame(columns=['geometry'], crs=config.ANALYSIS_CRS)
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
    logger.info("Suche White Spots in Berlin...")
    boundary = utils.get_berlin_boundary()
    
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

    output_path = config.GPKG_FILES["analysis_merged"]
    if os.path.exists(output_path): os.remove(output_path)
    gdf_final.to_file(output_path, layer="analyse_berlin", driver="GPKG")
    logger.info("✅ Fertig.")

if __name__ == "__main__":
    main()