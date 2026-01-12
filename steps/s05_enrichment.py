"""
enrichment step for geospatial data processing
1. load input data
2. classify land use priorities
3. process each district in parallel
4. calculate statistics
5. save output data
"""
import os
from typing import Optional, List

import geopandas as gpd
import pandas as pd
import numpy as np
import logging
import warnings
import traceback
from tqdm import tqdm
import ssl
import urllib.request
from concurrent.futures import ProcessPoolExecutor
from config import BASE_DIR, CRS, ENRICHMENT_INPUT_GPKG, ENRICHMENT_OUTPUT_GPKG, WFS_URLS, DISTRICT_MAPPING, get_log_path

LANDUSE_PRIORITY = {
    # HIGH POTENTIAL (Wohnen, Arbeit, Versorgung)
    "Wohnnutzung": "High",
    "Mischnutzung": "High",
    "Kerngebietsnutzung": "High",
    "Gewerbe- und Industrienutzung, großflächiger Einzelhandel": "High",
    "Gemeinbedarfs- und Sondernutzung": "High",
    "Bebauung mit überwiegender Nutzung durch Handel und Dienstleistung": "High",

    # MEDIUM POTENTIAL (Freizeit mit Gebäuden, Baustellen)
    "Wochenendhaus- und kleingartenähnliche Nutzung": "Medium",
    "Kleingartenanlage": "Medium",
    "Sportnutzung": "Medium",
    "Baustelle": "Medium",
    "Baumschule / Gartenbau": "Medium",
    "Ver- und Entsorgung": "Medium",
    "Sicherheit und Ordnung": "Medium",
    "Verwaltung": "Medium",
    "Kultur": "Medium",
    "Krankenhaus": "Medium",
    "Kindertagesstätte": "Medium",

    # LOW POTENTIAL (Natur, Infrastruktur ohne Gebäude)
    "Wald": "Low",
    "Grünland": "Low",
    "Ackerland": "Low",
    "Park / Grünfläche": "Low",
    "Friedhof": "Low",
    "Gewässer": "Low",
    "Brachfläche, Mischbestand aus Wiesen, Gebüschen und Bäumen": "Low",
    "Brachfläche, vegetationsfrei": "Low",
    "Brachfläche, wiesenartiger Vegetationsbestand": "Low",
    "Stadtplatz / Promenade": "Low",
    "Verkehrsfläche (ohne Straßen)": "Low",
    "sonstige Verkehrsfläche": "Low",
    "Parkplatz": "Low"}

    # Warnungen unterdrücken
warnings.filterwarnings("ignore")

INPUT_GPKG = ENRICHMENT_INPUT_GPKG
OUTPUT_GPKG = ENRICHMENT_OUTPUT_GPKG
LOG_FILE = get_log_path("05_enrichment.log")

def setup_logging() -> None:
    """
    sets up logging for the enrichment step

    Returns:
        None
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'), logging.StreamHandler()]
    )

def load_layer_safe(path: str, layer=None) -> gpd.GeoDataFrame:
    """
    loads a geospatial layer safely from a file

    Args:
        path (str): file path
        layer (str, optional): layer name for multi-layer files

    Returns:
        gdp.GeoDataFrame: loaded geodataframe or empty on error
    """
    if not os.path.exists(path):
        return gpd.GeoDataFrame()
    try:
        gdf = gpd.read_file(path, layer=layer, engine="pyogrio") if layer else gpd.read_file(path, engine="pyogrio")
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.error(f"Ladefehler {path}: {e}")
        return gpd.GeoDataFrame()

def get_wfs_data(url: str, name: str) -> Optional[gpd.GeoDataFrame]:
    """
    loads geodata from a wfs url safely

    Args:
        url (str): wfs url
        name (str): layer name for logging

    Returns:
        gpd.GeoDataFrame: loaded geodataframe or empty on error
    """
    logging.info(f"Lade {name} von GDI Berlin...")
    try:
        # ignore SSL certificate errors
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # creates a http handler with ssl context
        https_handler = urllib.request.HTTPSHandler(context=ssl_context)
        opener = urllib.request.build_opener(https_handler)
        urllib.request.install_opener(opener)

        gdf = gpd.read_file(url)
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.error(f"Download Fehler {name}: {e}")
        return gpd.GeoDataFrame()

def get_landuse_priority(row_series: pd.Series) -> tuple[str, str]:
    """
    determines land use priority from row "nutzung"
    1. check "nutzung" column
    2. fallback to "typklar" column -> default to "Unbekannt"
    3. map to priority using LANDUSE_PRIORITY dict

    Args:
        row_series (pd.Series): row data

    Returns:
        tuple[str, str]: priority level and cleaned land use value
    """
    val = "Unbekannt"
    if 'nutzung' in row_series and pd.notna(row_series['nutzung']):
        val = str(row_series['nutzung']).strip()
    elif 'typklar' in row_series and pd.notna(row_series['typklar']):
        val = str(row_series['typklar']).strip()

    priority = LANDUSE_PRIORITY.get(val, "Low")
    return priority, val

def simplify_fiber_status(status_str: str) -> str:
    """
    simplifies fiber status string to key categories

    Args:
        status_str (str): original status string

    Returns:
        str: simplified status category
    """
    s = str(status_str)
    if "Wettbewerb" in s: return "Wettbewerb"
    if "Telekom" in s and "Monopol" in s: return "Telekom"
    if "Vodafone" in s and "Monopol" in s: return "Vodafone"
    if "Planung" in s: return "Geplant"
    return "Kein Netz"

def process_district(args: tuple) -> List[gpd.GeoDataFrame]:
    """
    processes a single district for land use and fiber coverage
    1. pre-filtering
    2. clipping to district
    3. intersection with fiber data
    4. difference to find gaps

    Args:
        args (tuple): (bezirk_row, gdf_isu, gdf_fiber_active, bezirk_name)

    Returns:
        List[gdp.GeoDataFrame]: list of processed geodataframes for the district
    """
    bezirk_row, gdf_isu, gdf_fiber_active, bezirk_name = args
    bezirk_geom = bezirk_row.geometry
    
    results = []
    
    try:
        # 1. PRE-FILTERING
        minx, miny, maxx, maxy = bezirk_geom.bounds
        subset_isu = gdf_isu.cx[minx:maxx, miny:maxy]
        if subset_isu.empty: return []
            
        subset_fiber = gpd.GeoDataFrame()
        if not gdf_fiber_active.empty:
            subset_fiber = gdf_fiber_active.cx[minx:maxx, miny:maxy]

        # 2. CLIPPING
        mask = gpd.GeoDataFrame({'geometry': [bezirk_geom]}, crs=CRS)
        gdf_isu_bezirk = gpd.clip(subset_isu, mask)

        if gdf_isu_bezirk.empty: return []
        gdf_isu_bezirk = gdf_isu_bezirk[gdf_isu_bezirk.geom_type.isin(['Polygon', 'MultiPolygon'])]

        gdf_fiber_bezirk = gpd.GeoDataFrame()
        if not subset_fiber.empty:
            gdf_fiber_bezirk = gpd.clip(subset_fiber, mask)
            if not gdf_fiber_bezirk.empty:
                gdf_fiber_bezirk = gdf_fiber_bezirk[gdf_fiber_bezirk.geom_type.isin(['Polygon', 'MultiPolygon'])]

        # Geometry Fixes
        gdf_isu_bezirk['geometry'] = gdf_isu_bezirk.geometry.make_valid()
        if not gdf_fiber_bezirk.empty:
            gdf_fiber_bezirk['geometry'] = gdf_fiber_bezirk.geometry.make_valid()

        geo_col_isu = gdf_isu_bezirk.geometry.name

        # 3. INTERSECTION
        if not gdf_fiber_bezirk.empty:
            geo_col_fiber = gdf_fiber_bezirk.geometry.name
            gdf_intersect = gpd.overlay(
                gdf_isu_bezirk[['priority', 'nutzung_clean', geo_col_isu]],
                gdf_fiber_bezirk[['status', geo_col_fiber]], 
                how='intersection'
            )
            if not gdf_intersect.empty:
                gdf_intersect['versorgung_visual'] = gdf_intersect['status'].apply(simplify_fiber_status)
                results.append(gdf_intersect)

        # 4. DIFFERENCE
        gdf_gaps = gpd.GeoDataFrame()
        if not gdf_fiber_bezirk.empty:
            fiber_union = gdf_fiber_bezirk.dissolve()
            gdf_gaps = gpd.overlay(
                gdf_isu_bezirk[['priority', 'nutzung_clean', geo_col_isu]],
                fiber_union,
                how='difference'
            )
        else:
            gdf_gaps = gdf_isu_bezirk[['priority', 'nutzung_clean', geo_col_isu]].copy()
            
        if not gdf_gaps.empty:
            def label_gap(p):
                if p == "High": return "Potenzial (Hoch)"
                if p == "Medium": return "Potenzial (Mittel)"
                return "Potenzial (Niedrig)"

            gdf_gaps['versorgung_visual'] = gdf_gaps['priority'].apply(label_gap)
            gdf_gaps['status'] = "White Spot"
            results.append(gdf_gaps)

    except Exception as e:
        traceback.print_exc()
        return []

    final_results = []
    for gdf in results:
        gdf['bezirk_name'] = bezirk_name
        final_results.append(gdf)

    return final_results

def main() -> None:
    """
    main function for enrichment step

    Returns:
        None
    """
    logging.info("🚀 STARTE ENRICHMENT (V9.0 - District Mapping)")

    # 1. load data
    gdf_fiber = load_layer_safe(INPUT_GPKG, layer="analyse_berlin")
    if gdf_fiber.empty: return
    
    gdf_fiber_active = gdf_fiber[gdf_fiber['status'] != 'White Spot'].copy()
    
    gdf_bezirke = get_wfs_data(WFS_URLS["BEZIRKE"], "Bezirke")
    gdf_isu = get_wfs_data(WFS_URLS["ISU5"], "Flächennutzung")
    
    if gdf_isu.empty or gdf_bezirke.empty: return

    # --- BEZIRK MAPPING LOGIK ---
    logging.info("Wende DISTRICT_MAPPING an...")

    # Wir suchen die Spalte, die die IDs (z.B. '11000001') enthält
    id_col = None
    # Wir testen den ersten Wert jeder Spalte, ob er ein Key im Mapping ist
    sample_key = list(DISTRICT_MAPPING.keys())[0] # z.B. '11000001'

    for col in gdf_bezirke.columns:
        first_val = str(gdf_bezirke[col].iloc[0]).strip()
        # Check ob der Wert im Mapping existiert
        if first_val in DISTRICT_MAPPING:
            id_col = col
            break

    if id_col:
        logging.info(f"✅ ID-Spalte '{id_col}' gefunden. Mappe Namen...")
        # Neue Spalte 'clean_name' erzeugen
        gdf_bezirke['clean_name'] = gdf_bezirke[id_col].astype(str).str.strip().map(DISTRICT_MAPPING)
        # Falls Mapping lückenhaft ist, ID als Fallback
        gdf_bezirke['clean_name'] = gdf_bezirke['clean_name'].fillna(gdf_bezirke[id_col].astype(str))
    else:
        logging.warning("⚠️ Keine passende ID-Spalte für Mapping gefunden! Nutze Fallback-Suche.")
        # Fallback: Suche nach Spalte die 'name' heißt
        name_col = next((c for c in gdf_bezirke.columns if c.lower() in ['bezeichnung', 'name', 'nam']), 'id')
        gdf_bezirke['clean_name'] = gdf_bezirke[name_col]

    # 2. VORBEREITUNG ISU
    logging.info(f"Klassifiziere {len(gdf_isu)} Nutzungsblöcke...")
    if 'nutzung' not in gdf_isu.columns:
        logging.error("❌ CRITICAL: Spalte 'nutzung' fehlt!")
        return

    applied = gdf_isu.apply(get_landuse_priority, axis=1, result_type='expand')
    gdf_isu['priority'] = applied[0]
    gdf_isu['nutzung_clean'] = applied[1]
    
    logging.info("Prioritäten-Verteilung:")
    print(gdf_isu['priority'].value_counts())

    # 3. PARALLEL PROCESSING
    logging.info(f"Verarbeite 12 Bezirke...")
    
    if gdf_isu.sindex is None: pass
    if not gdf_fiber_active.empty and gdf_fiber_active.sindex is None: pass

    task_args = []
    # Wir iterieren jetzt über das GDF und nutzen unseren neuen 'clean_name'
    for _, row in gdf_bezirke.iterrows():
        b_name = row['clean_name'] # <--- HIER: Nutzung des gemappten Namens
        bbox = row.geometry.bounds
        isu_subset = gdf_isu.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy()
        fiber_subset = gdf_fiber_active.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy()
        if isu_subset.empty: continue
        task_args.append((row, isu_subset, fiber_subset, b_name))
        
    results_list = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = list(tqdm(executor.map(process_district, task_args), total=len(task_args), unit="bezirk", colour="green"))
        for res in futures: results_list.extend(res)

    # 4. MERGE & STATS
    if results_list:
        gdf_map_layer = pd.concat(results_list, ignore_index=True)
    else:
        return

    logging.info("Berechne Vertriebs-Statistiken...")

    stats_list = []
    # Alle Bezirke basierend auf unserer sauberen Namensliste
    all_bezirke = gdf_bezirke['clean_name'].unique()

    for b_name in all_bezirke:
        d_data = gdf_map_layer[gdf_map_layer['bezirk_name'] == b_name]
        if d_data.empty: continue

        def get_area(mask):
            return d_data[mask].geometry.area.sum() / 1e6

        mask_high_pot = d_data['versorgung_visual'] == "Potenzial (Hoch)"
        mask_mid_pot = d_data['versorgung_visual'] == "Potenzial (Mittel)"

        area_high_gap = get_area(mask_high_pot)
        area_mid_gap = get_area(mask_mid_pot)

        mask_high_total = d_data['priority'] == "High"
        mask_high_versorgt = mask_high_total & d_data['versorgung_visual'].isin(['Telekom', 'Vodafone', 'Wettbewerb'])
        area_high_total = get_area(mask_high_total)
        area_high_versorgt = get_area(mask_high_versorgt)

        versorgt_pct = 0
        if area_high_total > 0:
            versorgt_pct = round((area_high_versorgt / area_high_total * 100), 1)

        stats_list.append({
            "Bezirk": b_name,
            "Versorgt_High_Pct": versorgt_pct,
            "Gap_High_km2": round(area_high_gap, 2),
            "Gap_Mid_km2": round(area_mid_gap, 2)
        })

    df_stats = pd.DataFrame(stats_list)

    print("\n" + "="*70)
    print("💰 VERTRIEBS-POTENZIAL (Unversorgte Fläche in km²)")
    print("="*70)
    if not df_stats.empty:
        # Nach High Gap sortieren
        print(df_stats.sort_values('Gap_High_km2', ascending=False).to_string(index=False))
    print("="*70 + "\n")

    # 5. SPEICHERN
    if os.path.exists(OUTPUT_GPKG): os.remove(OUTPUT_GPKG)
    
    cols_export = ['priority', 'nutzung_clean', 'versorgung_visual', 'geometry']
    gdf_map_layer[cols_export].to_file(OUTPUT_GPKG, layer="map_detail_nutzung", driver="GPKG", engine="pyogrio")
    
    if not df_stats.empty:
        # Merge auf den sauberen Namen
        gdf_stats_out = gdf_bezirke.merge(df_stats, left_on="clean_name", right_on="Bezirk", how="left")
        gdf_stats_out.to_file(OUTPUT_GPKG, layer="map_stats_bezirke", driver="GPKG", engine="pyogrio")
    
    logging.info("✅ Fertig.")

if __name__ == "__main__":
    main()