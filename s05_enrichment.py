import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
import numpy as np
import logging
import warnings

# Warnungen bei GeoPandas Overlay unterdrücken
warnings.filterwarnings("ignore")

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
INPUT_GPKG = os.path.join(HAUPTORDNER, "04_analysis_merged.gpkg")
OUTPUT_GPKG = os.path.join(HAUPTORDNER, "05_master_analysis.gpkg")
LOG_DATEINAME = os.path.join(HAUPTORDNER, "05_enrichment.log")
ANALYSIS_CRS = "EPSG:25833" # UTM 33N (Berlin Standard)

# --- WFS QUELLEN (Berlin Open Data) ---
# Bezirke (Verwaltung)
URL_BEZIRKE = "https://gdi.berlin.de/services/wfs/alkis_bezirke?service=wfs&version=2.0.0&request=GetFeature&typeNames=alkis_bezirke:bezirksgrenzen&outputFormat=application/json&srsName=EPSG:25833"

# Flächennutzung (Umweltatlas)
URL_ISU5 = "https://gdi.berlin.de/services/wfs/ua_flaechennutzung?service=wfs&version=2.0.0&request=GetFeature&typeNames=ua_flaechennutzung:c_reale_nutzung_2023&outputFormat=application/json&srsName=EPSG:25833"

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(LOG_DATEINAME, mode='w', encoding='utf-8'), logging.StreamHandler()]
    )

def load_layer_safe(path, layer=None):
    if not os.path.exists(path):
        return gpd.GeoDataFrame()
    try:
        gdf = gpd.read_file(path, layer=layer) if layer else gpd.read_file(path)
        if gdf.crs != ANALYSIS_CRS:
            gdf = gdf.to_crs(ANALYSIS_CRS)
        return gdf
    except Exception as e:
        logging.error(f"Ladefehler {path}: {e}")
        return gpd.GeoDataFrame()

def get_wfs_data(url, name):
    logging.info(f"Lade {name} von GDI Berlin...")
    try:
        gdf = gpd.read_file(url)
        if gdf.crs != ANALYSIS_CRS:
            gdf = gdf.to_crs(ANALYSIS_CRS)
        return gdf
    except Exception as e:
        logging.error(f"Download Fehler {name}: {e}")
        return gpd.GeoDataFrame()

def determine_landuse_category(row, cols):
    """
    Intelligente Klassifizierung basierend auf ISU5 Codes.
    PRIORITÄT: Codes (nutzung) > Klartext (enutzung) > Struktur (typ)
    """
    
    # 1. VERSUCH: Nutzungscode (Am sichersten)
    # ISU5 Systematik: 1000=Wohnen, 2000=Gewerbe, 3000=Öffentlich, 4000=Verkehr
    nutzung_col = next((c for c in cols if c.lower() in ['nutzung', 'nutz', 'fl_nutz']), None)
    
    if nutzung_col:
        code = str(row[nutzung_col])
        if code.startswith('1'): return "Wohnen"
        if code.startswith('2'): return "Gewerbe"
        if code.startswith('3'): return "Öffentlich"
        # 4xxx ist Verkehr, 5xxx+ sind Grünflächen/Wasser -> Sonstiges
    
    # 2. VERSUCH: Nutzungstext (enutzung) - Falls Code fehlt
    text_col = next((c for c in cols if c.lower() in ['enutzung', 'nutzung_klar', 'nutz_klar']), None)
    
    if text_col:
        text = str(row[text_col]).lower()
        if "wohn" in text or "gemischt" in text: return "Wohnen"
        if "gewerbe" in text or "industrie" in text or "handel" in text or "dienstleist" in text or "kerngebiet" in text: return "Gewerbe"
        if "gemeinbedarf" in text or "schule" in text or "kultur" in text or "verwaltung" in text: return "Öffentlich"
    
    # Wenn weder Code noch Text passen:
    return "Sonstiges" # Wald, Wasser, Park, Friedhof, Verkehr

def simplify_fiber_status(status_str):
    s = str(status_str)
    if "Wettbewerb" in s: return "Wettbewerb"
    if "Telekom" in s and "Monopol" in s: return "Telekom"
    if "Vodafone" in s and "Monopol" in s: return "Vodafone"
    if "Planung" in s: return "Geplant"
    return "Kein Netz"

def main():
    setup_logging()
    logging.info("🚀 STARTE ENRICHMENT (V3 - Fix Klassifizierung)")

    # 1. DATEN LADEN
    gdf_fiber = load_layer_safe(INPUT_GPKG, layer="analyse_berlin")
    if gdf_fiber.empty:
        logging.error("Keine Glasfaser-Daten. Abbruch.")
        return
    
    gdf_bezirke = get_wfs_data(URL_BEZIRKE, "Bezirke")
    gdf_isu = get_wfs_data(URL_ISU5, "Flächennutzung")
    
    if gdf_isu.empty or gdf_bezirke.empty:
        logging.error("Basisdaten fehlen (WFS Fehler).")
        return

    # 2. VORBEREITUNG FLÄCHENNUTZUNG
    logging.info(f"Klassifiziere {len(gdf_isu)} Nutzungsblöcke...")
    logging.info(f"ISU Spalten (Check): {list(gdf_isu.columns)}")
    
    gdf_isu['kategorie'] = gdf_isu.apply(lambda row: determine_landuse_category(row, gdf_isu.columns), axis=1)
    gdf_isu['is_relevant'] = gdf_isu['kategorie'].isin(['Wohnen', 'Gewerbe', 'Öffentlich'])
    
    logging.info("Verteilung Nutzung (Neu):")
    # Zeige die Statistik im Log, damit wir sofort sehen, ob es geklappt hat
    counts = gdf_isu['kategorie'].value_counts()
    print(counts)
    
    if counts.get('Wohnen', 0) == 0 and counts.get('Gewerbe', 0) == 0:
        logging.error("❌ FEHLER: Immer noch alles 'Sonstiges'. Prüfe die 'nutzung' Spalte!")
        # Debugging: Zeige erste Zeilen der relevanten Spalten
        cols_debug = [c for c in gdf_isu.columns if 'nutz' in c.lower()]
        print("Beispiel-Daten (Nutzung):")
        print(gdf_isu[cols_debug].head())
        return

    # 3. LAYER 1: DIE VERSORGUNGS-KARTE
    logging.info("Erstelle Layer 1: Versorgungs-Karte (Verschneidung)...")
    
    gdf_fiber_active = gdf_fiber[gdf_fiber['status'] != 'White Spot'].copy()
    
    logging.info("   -> Verschneide Nutzung mit Infrastruktur...")
    gdf_intersect = gpd.overlay(
        gdf_isu[['kategorie', 'is_relevant', 'geometry']], 
        gdf_fiber_active[['status', 'geometry']], 
        how='intersection'
    )
    gdf_intersect['versorgung_visual'] = gdf_intersect['status'].apply(simplify_fiber_status)
    
    gdf_relevant_landuse = gdf_isu[gdf_isu['is_relevant'] == True]
    
    if gdf_relevant_landuse.empty:
        logging.warning("⚠️ ACHTUNG: Keine relevanten Nutzflächen gefunden! (Sollte mit Fix nicht mehr passieren)")
        gdf_gaps = gpd.GeoDataFrame()
    else:
        logging.info(f"   -> Berechne Versorgungslücken für {len(gdf_relevant_landuse)} relevante Blöcke...")
        if not gdf_fiber_active.empty:
            fiber_union = gdf_fiber_active.dissolve()
            gdf_gaps = gpd.overlay(gdf_relevant_landuse[['kategorie', 'is_relevant', 'geometry']], fiber_union, how='difference')
        else:
            gdf_gaps = gdf_relevant_landuse[['kategorie', 'is_relevant', 'geometry']].copy()
            
        gdf_gaps['versorgung_visual'] = "Lücke (White Spot)"
        gdf_gaps['status'] = "White Spot"
    
    gdf_map_layer = pd.concat([gdf_intersect, gdf_gaps], ignore_index=True)
    
    # 4. LAYER 2: BEZIRKS-STATISTIK
    logging.info("Erstelle Layer 2: Bezirks-Statistik...")
    
    # Robuste Spaltensuche für Bezirke
    bez_col = None
    candidates = ['bezeichnung', 'BEZ_NAME', 'bez_name', 'nam', 'name', 'bezirk', 'BEZEICHNUNG']
    for c in candidates:
        if c in gdf_bezirke.columns:
            bez_col = c
            break
            
    if not bez_col:
        # Fallback auf Index oder erste String-Spalte
        valid_cols = [c for c in gdf_bezirke.columns if c.lower() not in ['geometry', 'gml_id', 'id']]
        bez_col = valid_cols[0] if valid_cols else 'id'
        logging.info(f"Nutze Fallback-Spalte '{bez_col}' als Bezirksname.")

    if not gdf_map_layer.empty:
        gdf_map_layer['centroid'] = gdf_map_layer.geometry.centroid
        join_geo = gpd.GeoDataFrame(gdf_map_layer.drop(columns='geometry'), geometry='centroid', crs=ANALYSIS_CRS)
        
        joined = gpd.sjoin(join_geo, gdf_bezirke[[bez_col, 'geometry']], how='inner', predicate='within')
        joined['area_m2'] = gdf_map_layer.geometry.area
        
        stats = joined.groupby([bez_col, 'kategorie', 'versorgung_visual'])['area_m2'].sum().reset_index()
        
        districts_kpi = []
        for bezirk in stats[bez_col].unique():
            d_data = stats[stats[bez_col] == bezirk]
            
            def get_area(kat, versorgung_list):
                mask = (d_data['kategorie'] == kat) & (d_data['versorgung_visual'].isin(versorgung_list))
                return d_data[mask]['area_m2'].sum() / 1_000_000 # km²

            wohn_total = get_area("Wohnen", ["Telekom", "Vodafone", "Wettbewerb", "Geplant", "Lücke (White Spot)"])
            wohn_versorgt = get_area("Wohnen", ["Telekom", "Vodafone", "Wettbewerb"])
            wohn_gap = get_area("Wohnen", ["Lücke (White Spot)"])
            
            gew_total = get_area("Gewerbe", ["Telekom", "Vodafone", "Wettbewerb", "Geplant", "Lücke (White Spot)"])
            gew_versorgt = get_area("Gewerbe", ["Telekom", "Vodafone", "Wettbewerb"])
            
            kpi = {
                "Bezirk": bezirk,
                "Wohnflaeche_Total_km2": round(wohn_total, 2),
                "Wohn_Versorgt_Pct": round((wohn_versorgt / wohn_total * 100), 1) if wohn_total > 0 else 0,
                "Wohn_Luecke_km2": round(wohn_gap, 2),
                "Gewerbe_Versorgt_Pct": round((gew_versorgt / gew_total * 100), 1) if gew_total > 0 else 0
            }
            districts_kpi.append(kpi)
            
        df_district_stats = pd.DataFrame(districts_kpi)
        gdf_district_stats = gdf_bezirke[[bez_col, 'geometry']].merge(df_district_stats, left_on=bez_col, right_on="Bezirk")

        print("\n" + "="*60)
        print("📊 BEZIRKS-RANKING (Wohn-Versorgung)")
        print("="*60)
        if not df_district_stats.empty:
            print(df_district_stats[['Bezirk', 'Wohn_Versorgt_Pct', 'Gewerbe_Versorgt_Pct', 'Wohn_Luecke_km2']].sort_values('Wohn_Versorgt_Pct', ascending=False).to_string(index=False))
        print("="*60 + "\n")
    else:
        gdf_district_stats = gpd.GeoDataFrame()

    # 5. SPEICHERN
    if os.path.exists(OUTPUT_GPKG): os.remove(OUTPUT_GPKG)
    
    logging.info(f"Speichere Ergebnisse in {OUTPUT_GPKG}...")
    
    if not gdf_map_layer.empty:
        cols_export = ['kategorie', 'versorgung_visual', 'is_relevant', 'geometry']
        gdf_map_layer[cols_export].to_file(OUTPUT_GPKG, layer="map_detail_nutzung", driver="GPKG")
    
    if not gdf_district_stats.empty:
        gdf_district_stats.to_file(OUTPUT_GPKG, layer="map_stats_bezirke", driver="GPKG")
    
    logging.info("✅ Fertig.")

if __name__ == "__main__":
    main()