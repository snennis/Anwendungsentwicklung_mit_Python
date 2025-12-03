import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
from shapely.geometry import box
import logging
from typing import Dict

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
OUTPUT_GPKG = os.path.join(HAUPTORDNER, "analysis_results.gpkg")
LOG_DATEINAME = os.path.join(HAUPTORDNER, "analysis.log")

# Wir nutzen EPSG:25833 (UTM 33N) für Berlin/BB für korrekte Flächenberechnungen
ANALYSIS_CRS = "EPSG:25833" 

# Dateinamen aus Schritt 02 (Namen müssen übereinstimmen!)
INPUT_FILES = {
    "tk_2000": "tk_fiber_2000.gpkg",
    "tk_1000": "tk_fiber_1000.gpkg",
    "tk_plan": "tk_fiber_plan.gpkg",
    "vf_1000": "vf_fiber_1000.gpkg"
}

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_DATEINAME, mode='w'),
            logging.StreamHandler()
        ]
    )

def load_layer(key: str) -> gpd.GeoDataFrame:
    """Lädt ein GPKG, repariert Geometrien und reprojiziert."""
    filepath = os.path.join(HAUPTORDNER, INPUT_FILES[key])
    if not os.path.exists(filepath):
        logging.warning(f"Datei nicht gefunden: {filepath}. Gebe leeres GDF zurück.")
        return gpd.GeoDataFrame(columns=['geometry', 'category'], crs=ANALYSIS_CRS)
    
    logging.info(f"Lade {key}...")
    gdf = gpd.read_file(filepath)
    
    # 1. Reprojektion (wichtig für Metrische Operationen)
    if gdf.crs != ANALYSIS_CRS:
        gdf = gdf.to_crs(ANALYSIS_CRS)
    
    # 2. Geometrie-Fix (Buffer 0 repariert Self-Intersections)
    gdf['geometry'] = gdf.geometry.buffer(0)
    
    # 3. Validitäts-Check
    gdf = gdf[gdf.is_valid]
    
    return gdf

def get_boundary_bb():
    """Holt die Grenze von Berlin & Brandenburg via OSMnx."""
    logging.info("Lade Verwaltungsgrenzen von Berlin und Brandenburg via OSMnx...")
    try:
        # Wir laden beide und vereinen sie
        gdf_berlin = ox.geocode_to_gdf("Berlin, Germany")
        gdf_brandenburg = ox.geocode_to_gdf("Brandenburg, Germany")
        
        gdf_total = pd.concat([gdf_berlin, gdf_brandenburg])
        gdf_total = gdf_total.to_crs(ANALYSIS_CRS)
        
        # Vereinen zu einem einzigen Polygon (Dissolve)
        boundary = gdf_total.dissolve()
        return boundary
    except Exception as e:
        logging.error(f"Konnte Grenzen nicht laden: {e}")
        logging.warning("Nutze Bounding Box als Fallback.")
        # Fallback BBox (ungefähr BB)
        bbox = box(1250000, 6750000, 1660000, 7080000) # Deine BBox Werte
        return gpd.GeoDataFrame({'geometry': [bbox]}, crs="EPSG:3857").to_crs(ANALYSIS_CRS)

def calculate_area_km2(gdf):
    if gdf.empty: return 0.0
    return gdf.geometry.area.sum() / 1_000_000 # m² zu km²

def main():
    setup_logging()
    logging.info("🚀 Starte Analyse-Phase (Mengenlehre & Geoprozessierung)")

    # 1. DATEN LADEN
    gdf_tk_2000 = load_layer("tk_2000")
    gdf_tk_1000 = load_layer("tk_1000")
    gdf_tk_plan = load_layer("tk_plan")
    gdf_vf_1000 = load_layer("vf_1000")

    # 2. AGGREGATION (Telekom Gesamt Status Quo)
    logging.info("Aggregiere Telekom Bestand (1000 + 2000)...")
    # Wir kombinieren 1000 und 2000 zu "Telekom Bestand"
    gdf_tk_total = pd.concat([gdf_tk_2000, gdf_tk_1000])
    if not gdf_tk_total.empty:
        gdf_tk_total = gdf_tk_total.dissolve() # Geometrien verschmelzen
    
    # Vodafone Bestand (auch dissolven für saubere Kanten)
    if not gdf_vf_1000.empty:
        gdf_vf_1000 = gdf_vf_1000.dissolve()

    # 3. ANALYSE: WETTBEWERB VS MONOPOL
    logging.info("Berechne Marktsituation...")
    
    # A) Wettbewerb (Schnittmenge)
    if not gdf_tk_total.empty and not gdf_vf_1000.empty:
        gdf_competition = gpd.overlay(gdf_tk_total, gdf_vf_1000, how='intersection')
        gdf_competition['status'] = 'Wettbewerb (TK & VF)'
    else:
        gdf_competition = gpd.GeoDataFrame(columns=['geometry', 'status'], crs=ANALYSIS_CRS)

    # B) Monopol Telekom (TK ohne VF)
    if not gdf_tk_total.empty:
        if not gdf_vf_1000.empty:
            gdf_monopol_tk = gpd.overlay(gdf_tk_total, gdf_vf_1000, how='difference')
        else:
            gdf_monopol_tk = gdf_tk_total # Alles ist Monopol wenn VF leer
        gdf_monopol_tk['status'] = 'Monopol Telekom'
    else:
        gdf_monopol_tk = gpd.GeoDataFrame(columns=['geometry', 'status'], crs=ANALYSIS_CRS)

    # C) Monopol Vodafone (VF ohne TK)
    if not gdf_vf_1000.empty:
        if not gdf_tk_total.empty:
            gdf_monopol_vf = gpd.overlay(gdf_vf_1000, gdf_tk_total, how='difference')
        else:
            gdf_monopol_vf = gdf_vf_1000
        gdf_monopol_vf['status'] = 'Monopol Vodafone'
    else:
        gdf_monopol_vf = gpd.GeoDataFrame(columns=['geometry', 'status'], crs=ANALYSIS_CRS)

    # 4. ANALYSE: GEPLANT VS BESTAND
    logging.info("Analysiere Ausbau-Pläne...")
    # Wo plant TK, wo es schon VF gibt? (Strategischer Überbau?)
    if not gdf_tk_plan.empty and not gdf_vf_1000.empty:
        gdf_plan_overbuild = gpd.overlay(gdf_tk_plan, gdf_vf_1000, how='intersection')
        gdf_plan_overbuild['status'] = 'TK plant in VF Gebiet'
        
        # Wo plant TK, wo noch gar nichts ist?
        # (TK Plan) MINUS (VF Bestand) MINUS (TK Bestand)
        existing_infrastructure = pd.concat([gdf_tk_total, gdf_vf_1000]).dissolve()
        gdf_plan_new = gpd.overlay(gdf_tk_plan, existing_infrastructure, how='difference')
        gdf_plan_new['status'] = 'TK erschließt Neuland'
    else:
        gdf_plan_overbuild = gpd.GeoDataFrame(columns=['geometry'], crs=ANALYSIS_CRS)
        gdf_plan_new = gdf_tk_plan

    # 5. ANALYSE: WHITE SPOTS (Versorgungslücken)
    logging.info("Berechne White Spots (Versorgungslücken)...")
    boundary = get_boundary_bb()
    
    # Alles was wir haben (TK Bestand + TK Plan + VF Bestand)
    all_coverage = pd.concat([gdf_tk_total, gdf_tk_plan, gdf_vf_1000])
    if not all_coverage.empty:
        all_coverage_union = all_coverage.dissolve()
        # White Spots = Landesfläche MINUS Abdeckung
        gdf_white_spots = gpd.overlay(boundary, all_coverage_union, how='difference')
        gdf_white_spots['status'] = 'Kein Glasfaser (White Spot)'
    else:
        gdf_white_spots = boundary # Alles ist White Spot

    # 6. REPORTING & EXPORT
    logging.info("Erstelle Statistik und speichere Ergebnisse...")
    
    stats = {
        "Telekom 2000": calculate_area_km2(gdf_tk_2000),
        "Telekom 1000": calculate_area_km2(gdf_tk_1000),
        "Vodafone 1000": calculate_area_km2(gdf_vf_1000),
        "---": "---",
        "Wettbewerb (Overlap)": calculate_area_km2(gdf_competition),
        "Monopol Telekom": calculate_area_km2(gdf_monopol_tk),
        "Monopol Vodafone": calculate_area_km2(gdf_monopol_vf),
        "---": "---",
        "TK plant Überbau": calculate_area_km2(gdf_plan_overbuild),
        "TK plant Neuland": calculate_area_km2(gdf_plan_new),
        "White Spots": calculate_area_km2(gdf_white_spots)
    }

    print("\n" + "="*40)
    print(f"📊 ANALYSE ERGEBNISSE (in km²)")
    print("="*40)
    for k, v in stats.items():
        if v == "---":
            print("-" * 20)
        else:
            print(f"{k:<25}: {v:>10.2f} km²")
    print("="*40 + "\n")

    # Speichern in EINEM GeoPackage mit mehreren Layern
    if os.path.exists(OUTPUT_GPKG): os.remove(OUTPUT_GPKG)
    
    # Funktion zum sicheren Speichern (überspringt leere GDFs)
    def save_layer(gdf, name):
        if not gdf.empty:
            gdf.to_file(OUTPUT_GPKG, layer=name, driver="GPKG")
            logging.info(f"Layer gespeichert: {name}")

    save_layer(gdf_competition, "analyse_wettbewerb")
    save_layer(gdf_monopol_tk, "analyse_monopol_telekom")
    save_layer(gdf_monopol_vf, "analyse_monopol_vodafone")
    save_layer(gdf_plan_overbuild, "analyse_plan_ueberbau")
    save_layer(gdf_plan_new, "analyse_plan_neuland")
    save_layer(gdf_white_spots, "analyse_white_spots")
    
    # Original Layer auch reinpacken für Referenz? Optional.
    # save_layer(gdf_tk_total, "ref_telekom_total")
    # save_layer(gdf_vf_1000, "ref_vodafone_total")

    logging.info(f"✅ Analyse abgeschlossen. Datei: {OUTPUT_GPKG}")

if __name__ == "__main__":
    main()