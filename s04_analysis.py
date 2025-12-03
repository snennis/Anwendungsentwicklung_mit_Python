import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
from shapely.geometry import box
import logging

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
OUTPUT_GPKG = os.path.join(HAUPTORDNER, "04_analysis_merged.gpkg") # Neuer Name
LOG_DATEINAME = os.path.join(HAUPTORDNER, "04_analysis.log")
ANALYSIS_CRS = "EPSG:25833" 

# Wir laden die CLEAN Files aus Schritt 03
INPUT_FILES = {
    "tk_2000": "clean_tk_2000.gpkg",
    "tk_1000": "clean_tk_1000.gpkg",
    "tk_plan": "clean_tk_plan.gpkg",
    "vf_1000": "clean_vf_1000.gpkg"
}

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s',
                        handlers=[logging.FileHandler(LOG_DATEINAME, mode='w'), logging.StreamHandler()])

def load_clean_layer(key: str) -> gpd.GeoDataFrame:
    """Lädt die bereits bereinigten Daten."""
    filepath = os.path.join(HAUPTORDNER, INPUT_FILES[key])
    if not os.path.exists(filepath):
        return gpd.GeoDataFrame(columns=['geometry', 'category'], crs=ANALYSIS_CRS)
    
    logging.info(f"Lade {key}...")
    gdf = gpd.read_file(filepath)
    if gdf.crs != ANALYSIS_CRS:
        gdf = gdf.to_crs(ANALYSIS_CRS)
    return gdf

def get_boundary_bb():
    logging.info("Lade Grenzen (Berlin/Brandenburg)...")
    try:
        gdf_berlin = ox.geocode_to_gdf("Berlin, Germany")
        gdf_brandenburg = ox.geocode_to_gdf("Brandenburg, Germany")
        boundary = pd.concat([gdf_berlin, gdf_brandenburg]).to_crs(ANALYSIS_CRS).dissolve()
        return boundary
    except:
        logging.warning("OSM Fehler. Nutze BBox.")
        bbox = box(1250000, 6750000, 1660000, 7080000) 
        return gpd.GeoDataFrame({'geometry': [bbox]}, crs="EPSG:3857").to_crs(ANALYSIS_CRS)

def calculate_area_km2(gdf):
    if gdf.empty: return 0.0
    return gdf.geometry.area.sum() / 1_000_000

def main():
    setup_logging()
    logging.info("🚀 Starte Analyse (Merged Output)")

    # 1. LADEN
    gdf_tk_2000 = load_clean_layer("tk_2000")
    gdf_tk_1000 = load_clean_layer("tk_1000")
    gdf_tk_plan = load_clean_layer("tk_plan")
    gdf_vf_1000 = load_clean_layer("vf_1000")

    # 2. AGGREGATION (Telekom Gesamt)
    logging.info("Verschmelze Telekom Bestand...")
    gdf_tk_total = pd.concat([gdf_tk_2000, gdf_tk_1000])
    if not gdf_tk_total.empty: gdf_tk_total = gdf_tk_total.dissolve()

    # 3. WETTBEWERB & MONOPOLE (Status Quo)
    logging.info("Analysiere Markt...")
    
    # Init leere GDFs
    gdf_competition = gpd.GeoDataFrame(columns=['geometry'], crs=ANALYSIS_CRS)
    gdf_monopol_tk = gdf_tk_total.copy()
    gdf_monopol_vf = gdf_vf_1000.copy()

    if not gdf_tk_total.empty and not gdf_vf_1000.empty:
        # A) Wettbewerb
        gdf_competition = gpd.overlay(gdf_tk_total, gdf_vf_1000, how='intersection')
        
        # B) Monopol TK (Difference)
        gdf_monopol_tk = gpd.overlay(gdf_tk_total, gdf_vf_1000, how='difference')
        
        # C) Monopol VF (Difference)
        gdf_monopol_vf = gpd.overlay(gdf_vf_1000, gdf_tk_total, how='difference')

    # Attribute setzen (Wichtig für den Merge!)
    gdf_competition['status'] = 'Wettbewerb'
    gdf_competition['type'] = 'Bestand'
    
    gdf_monopol_tk['status'] = 'Monopol Telekom'
    gdf_monopol_tk['type'] = 'Bestand'
    
    gdf_monopol_vf['status'] = 'Monopol Vodafone'
    gdf_monopol_vf['type'] = 'Bestand'

    # 4. WHITE SPOTS
    logging.info("Suche White Spots...")
    boundary = get_boundary_bb()
    all_infra = pd.concat([gdf_tk_total, gdf_tk_plan, gdf_vf_1000])
    
    if not all_infra.empty:
        gdf_white = gpd.overlay(boundary, all_infra.dissolve(), how='difference')
    else:
        gdf_white = boundary
        
    gdf_white['status'] = 'White Spot'
    gdf_white['type'] = 'Lücke'

    # 5. PLANUNG (Optionaler Overlay)
    # Wir fügen die Planung auch hinzu. Achtung: Diese Polygone können über den anderen liegen!
    # Das ist in einem GPKG okay.
    if not gdf_tk_plan.empty:
        gdf_tk_plan['status'] = 'Telekom Planung'
        gdf_tk_plan['type'] = 'Planung'

    # 6. ZUSAMMENFÜHREN (MERGE)
    logging.info("Führe alle Ergebnisse in einen Layer zusammen...")
    
    # Liste aller Teilergebnisse
    all_results = [
        gdf_competition,
        gdf_monopol_tk,
        gdf_monopol_vf,
        gdf_white,
        gdf_tk_plan
    ]
    
    # Filtern (nur nicht-leere) und Concatenaten
    gdf_final = pd.concat([g for g in all_results if not g.empty], ignore_index=True)

    # Aufräumen: Nur relevante Spalten behalten
    cols_to_keep = ['geometry', 'status', 'type']
    # Falls noch alte Spalten da sind (category etc.), ignorieren wir sie
    gdf_final = gdf_final[cols_to_keep]

    # STATISTIK (Terminal Output)
    print("\n" + "="*30)
    print("📊 STATISTIK (Merged Layer)")
    print("="*30)
    # Groupby ist jetzt super einfach:
    stats = gdf_final.dissolve(by='status').area / 1_000_000
    print(stats.round(2))
    print("="*30 + "\n")

    # SPEICHERN
    if os.path.exists(OUTPUT_GPKG): os.remove(OUTPUT_GPKG)
    
    logging.info(f"Speichere {len(gdf_final)} Objekte in {OUTPUT_GPKG}...")
    gdf_final.to_file(OUTPUT_GPKG, layer="gesamt_analyse", driver="GPKG")

    logging.info("✅ Fertig.")

if __name__ == "__main__":
    main()