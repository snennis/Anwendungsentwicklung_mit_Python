import os
import geopandas as gpd
import pandas as pd
import numpy as np
import logging
import warnings
import ssl
import traceback
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from config import BASE_DIR, get_log_path, CRS, ENRICHMENT_INPUT_GPKG, ENRICHMENT_OUTPUT_GPKG, WFS_URLS

# --- SSL-HACK START ---
ssl._create_default_https_context = ssl._create_unverified_context
# --- SSL-HACK END ---

# Warnungen bei GeoPandas Overlay unterdrücken
warnings.filterwarnings("ignore")

INPUT_GPKG = ENRICHMENT_INPUT_GPKG
OUTPUT_GPKG = ENRICHMENT_OUTPUT_GPKG
# LOG_FILE removed

# setup_logging removed

def load_layer_safe(path, layer=None):
    if not os.path.exists(path):
        return gpd.GeoDataFrame()
    try:
        # Use pyogrio for speed
        gdf = gpd.read_file(path, layer=layer, engine="pyogrio") if layer else gpd.read_file(path, engine="pyogrio")
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.error(f"Ladefehler {path}: {e}")
        return gpd.GeoDataFrame()

def get_wfs_data(url, name):
    logging.info(f"Lade {name} von GDI Berlin...")
    try:
        gdf = gpd.read_file(url)
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.error(f"Download Fehler {name}: {e}")
        return gpd.GeoDataFrame()

def determine_landuse_category(row_series, cols):
    """
    Intelligente Klassifizierung basierend auf ISU5 Codes.
    """
    # 1. VERSUCH: Nutzungscode (Am sichersten)
    nutzung_col = next((c for c in cols if c.lower() in ['nutzung', 'nutz', 'fl_nutz']), None)
    
    if nutzung_col:
        code = str(row_series[nutzung_col])
        if code.startswith('1'): return "Wohnen"
        if code.startswith('2'): return "Gewerbe"
        if code.startswith('3'): return "Öffentlich"
    
    # 2. VERSUCH: Nutzungstext
    text_col = next((c for c in cols if c.lower() in ['enutzung', 'nutzung_klar', 'nutz_klar']), None)
    
    if text_col:
        text = str(row_series[text_col]).lower()
        if "wohn" in text or "gemischt" in text: return "Wohnen"
        if "gewerbe" in text or "industrie" in text or "handel" in text or "dienstleist" in text or "kerngebiet" in text: return "Gewerbe"
        if "gemeinbedarf" in text or "schule" in text or "kultur" in text or "verwaltung" in text: return "Öffentlich"
    
    return "Sonstiges"

def simplify_fiber_status(status_str):
    s = str(status_str)
    if "Wettbewerb" in s: return "Wettbewerb"
    if "Telekom" in s and "Monopol" in s: return "Telekom"
    if "Vodafone" in s and "Monopol" in s: return "Vodafone"
    if "Planung" in s: return "Geplant"
    return "Kein Netz"

def process_district(args):
    """
    Processes a single district.
    Args: (bezirk_row, gdf_isu, gdf_fiber_active)
    """
    bezirk_row, gdf_isu, gdf_fiber_active = args
    bezirk_geom = bezirk_row.geometry
    
    results = []
    
    try:
        # 1. PRE-FILTERING (Spatial Index)
        minx, miny, maxx, maxy = bezirk_geom.bounds
        subset_isu = gdf_isu.cx[minx:maxx, miny:maxy]
        
        if subset_isu.empty:
            return []
            
        subset_fiber = gpd.GeoDataFrame()
        if not gdf_fiber_active.empty:
            subset_fiber = gdf_fiber_active.cx[minx:maxx, miny:maxy]

        # 2. CLIPPING
        mask = gpd.GeoSeries([bezirk_geom], crs=CRS)
        
        gdf_isu_bezirk = gpd.clip(subset_isu, mask)
        if gdf_isu_bezirk.empty: return []

        gdf_isu_bezirk = gdf_isu_bezirk[gdf_isu_bezirk.geom_type.isin(['Polygon', 'MultiPolygon'])]
        if gdf_isu_bezirk.empty: return []

        gdf_fiber_bezirk = gpd.GeoDataFrame()
        if not subset_fiber.empty:
            gdf_fiber_bezirk = gpd.clip(subset_fiber, mask)
            if not gdf_fiber_bezirk.empty:
                gdf_fiber_bezirk = gdf_fiber_bezirk[gdf_fiber_bezirk.geom_type.isin(['Polygon', 'MultiPolygon'])]

        # 3. INTERSECTION
        gdf_intersect = gpd.GeoDataFrame()
        
        if not gdf_fiber_bezirk.empty:
            # --- FIX: make_valid korrekt zuweisen ---
            # Wir weisen es der Geometrie-Spalte zu, statt das DF zu überschreiben
            try:
                gdf_isu_bezirk[gdf_isu_bezirk.geometry.name] = gdf_isu_bezirk.geometry.make_valid()
                gdf_fiber_bezirk[gdf_fiber_bezirk.geometry.name] = gdf_fiber_bezirk.geometry.make_valid()
            except AttributeError:
                # Fallback für ältere GeoPandas Versionen
                gdf_isu_bezirk[gdf_isu_bezirk.geometry.name] = gdf_isu_bezirk.geometry.buffer(0)
                gdf_fiber_bezirk[gdf_fiber_bezirk.geometry.name] = gdf_fiber_bezirk.geometry.buffer(0)

            # Sicherstellen, dass wir den richtigen Geometrie-Namen nutzen
            geo_col_isu = gdf_isu_bezirk.geometry.name
            geo_col_fiber = gdf_fiber_bezirk.geometry.name

            gdf_intersect = gpd.overlay(
                gdf_isu_bezirk[['kategorie', 'is_relevant', geo_col_isu]], 
                gdf_fiber_bezirk[['status', geo_col_fiber]], 
                how='intersection'
            )
            if not gdf_intersect.empty:
                gdf_intersect['versorgung_visual'] = gdf_intersect['status'].apply(simplify_fiber_status)
        
        # 4. DIFFERENCE (White Spots)
        gdf_gaps = gpd.GeoDataFrame()
        gdf_relevant_bezirk = gdf_isu_bezirk[gdf_isu_bezirk['is_relevant'] == True]
        
        if not gdf_relevant_bezirk.empty:
            geo_col_rel = gdf_relevant_bezirk.geometry.name
            
            if not gdf_fiber_bezirk.empty:
                fiber_union = gdf_fiber_bezirk.dissolve()
                gdf_gaps = gpd.overlay(
                    gdf_relevant_bezirk[['kategorie', 'is_relevant', geo_col_rel]], 
                    fiber_union, 
                    how='difference'
                )
            else:
                gdf_gaps = gdf_relevant_bezirk[['kategorie', 'is_relevant', geo_col_rel]].copy()
            
            if not gdf_gaps.empty:
                gdf_gaps['versorgung_visual'] = "Lücke (White Spot)"
                gdf_gaps['status'] = "White Spot"

        # 5. RESULT COLLECTION
        if not gdf_intersect.empty:
            results.append(gdf_intersect)
        if not gdf_gaps.empty:
            results.append(gdf_gaps)
            
    except Exception as e:
        print(f"❌ ERROR in District {bezirk_row.get('nam', 'Unknown')}:")
        traceback.print_exc()
        return []
        
    return results

def main():
    # setup_logging() handled by main.py
    logging.info("🚀 STARTE ENRICHMENT (V5.2 - Fixed Geometry Logic)")

    # 1. DATEN LADEN
    gdf_fiber = load_layer_safe(INPUT_GPKG, layer="analyse_berlin")
    if gdf_fiber.empty:
        logging.error("Keine Glasfaser-Daten. Abbruch.")
        return
    
    gdf_fiber_active = gdf_fiber[gdf_fiber['status'] != 'White Spot'].copy()
    
    gdf_bezirke = get_wfs_data(WFS_URLS["BEZIRKE"], "Bezirke")
    gdf_isu = get_wfs_data(WFS_URLS["ISU5"], "Flächennutzung")
    
    if gdf_isu.empty or gdf_bezirke.empty:
        logging.error("Basisdaten fehlen (WFS Fehler).")
        return

    # 2. VORBEREITUNG
    logging.info(f"Klassifiziere {len(gdf_isu)} Nutzungsblöcke...")
    gdf_isu['kategorie'] = gdf_isu.apply(lambda row: determine_landuse_category(row, gdf_isu.columns), axis=1)
    gdf_isu['is_relevant'] = gdf_isu['kategorie'].isin(['Wohnen', 'Gewerbe', 'Öffentlich'])
    
    # 3. PARALLEL PROCESSING
    logging.info(f"🚀 Starte parallele Verarbeitung von {len(gdf_bezirke)} Bezirken...")
    
    # Spatial Index vorbauen (verhindert Race-Conditions)
    try:
        if gdf_isu.sindex is None: pass 
        if not gdf_fiber_active.empty and gdf_fiber_active.sindex is None: pass
    except: pass

    results_list = []
    task_args = []
    for _, row in gdf_bezirke.iterrows():
        task_args.append((row, gdf_isu, gdf_fiber_active))
        
    max_workers = min(os.cpu_count(), len(gdf_bezirke))
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = list(tqdm(
            executor.map(process_district, task_args), 
            total=len(task_args), 
            unit="bezirk", 
            colour="green", 
            desc="Verarbeite"
        ))
        
    for district_res in futures:
        results_list.extend(district_res)

    # 4. ZUSAMMENFÜHREN
    logging.info("Führe Ergebnisse zusammen...")
    if results_list:
        gdf_map_layer = pd.concat(results_list, ignore_index=True)
    else:
        logging.warning("⚠️ Keine Ergebnisse generiert!")
        gdf_map_layer = gpd.GeoDataFrame()

    # 5. STATISTIKEN
    logging.info("Berechne finale Statistiken...")
    bez_col = next((c for c in gdf_bezirke.columns if c.lower() in ['bezeichnung', 'name', 'nam']), 'id')
    gdf_district_stats = gpd.GeoDataFrame()

    if not gdf_map_layer.empty:
        gdf_map_layer['centroid'] = gdf_map_layer.geometry.centroid
        join_geo = gpd.GeoDataFrame(gdf_map_layer.drop(columns='geometry'), geometry='centroid', crs=CRS)
        
        joined = gpd.sjoin(join_geo, gdf_bezirke[[bez_col, 'geometry']], how='inner', predicate='within')
        joined['area_m2'] = gdf_map_layer.geometry.area
        
        stats = joined.groupby([bez_col, 'kategorie', 'versorgung_visual'])['area_m2'].sum().reset_index()
        
        districts_kpi = []
        for bezirk in stats[bez_col].unique():
            d_data = stats[stats[bez_col] == bezirk]
            
            def get_area(kat, versorgung_list):
                mask = (d_data['kategorie'] == kat) & (d_data['versorgung_visual'].isin(versorgung_list))
                return d_data[mask]['area_m2'].sum() / 1_000_000 

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
        print(df_district_stats[['Bezirk', 'Wohn_Versorgt_Pct', 'Wohn_Luecke_km2']].sort_values('Wohn_Versorgt_Pct', ascending=False).to_string(index=False))
        print("="*60 + "\n")

    # 6. SPEICHERN
    if os.path.exists(OUTPUT_GPKG): os.remove(OUTPUT_GPKG)
    
    if not gdf_map_layer.empty:
        logging.info(f"Speichere Ergebnisse in {OUTPUT_GPKG}...")
        cols_export = ['kategorie', 'versorgung_visual', 'is_relevant', 'geometry']
        gdf_map_layer[cols_export].to_file(OUTPUT_GPKG, layer="map_detail_nutzung", driver="GPKG", engine="pyogrio")
    
    if not gdf_district_stats.empty:
        gdf_district_stats.to_file(OUTPUT_GPKG, layer="map_stats_bezirke", driver="GPKG", engine="pyogrio")
    
    logging.info("✅ Fertig.")

if __name__ == "__main__":
    main()