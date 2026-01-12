"""
analysis of telekom and vodafone coverage in berlin.
1. load data
2. split by districts
3. analyze each district in parallel
4. merge results
"""
import os
import geopandas as gpd
import pandas as pd
import logging
import warnings
from shapely.geometry import box
from shapely.ops import unary_union
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import ssl
import urllib.request
from config import BASE_DIR, CRS, ANALYSIS_INPUT_FILES, ANALYSIS_OUTPUT_GPKG, WFS_URLS

# ignore warning
warnings.filterwarnings("ignore")

def load_layer_safe(key: str) -> gpd.GeoDataFrame:
    """
    loads a layer from disk safely, returns empty gdf on failure
    1. checks if file exists
    2. loads with geopandas
    3. reprojects to crs if necessary
    4. returns gdf
    5. on failure returns empty gdf

    Args:
        key (str): key in ANALYSIS_INPUT_FILES dict

    Returns:
        gpd.GeoDataFrame: loaded layer or empty gdf
    """
    filepath = ANALYSIS_INPUT_FILES.get(key)
    if not filepath or not os.path.exists(filepath):
        return gpd.GeoDataFrame(columns=['geometry'], crs=CRS)
    
    logging.info(f"Lade {key}...")
    try:
        gdf = gpd.read_file(filepath, engine="pyogrio")
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.error(f"Fehler beim Laden von {key}: {e}")
        return gpd.GeoDataFrame(columns=['geometry'], crs=CRS)

def load_districts_for_splitting() -> gpd.GeoDataFrame:
    """
    loads berlin districts from WFS
    1. tries to load from WFS
    2. on failure returns fallback bbox as single district
    3. reprojects to crs if necessary
    4. returns gdf

    Returns:
        gdp.GeoDataFrame: loaded districts or fallback bbox
    """
    logging.info("Lade Bezirksgrenzen für Partitionierung...")
    try:
        # deactivate SSL verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # HTTPS handler with SSL-context
        https_handler = urllib.request.HTTPSHandler(context=ssl_context)
        opener = urllib.request.build_opener(https_handler)
        urllib.request.install_opener(opener)

        gdf = gpd.read_file(WFS_URLS["BEZIRKE"])
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.warning(f"Konnte Bezirke nicht laden ({e}). Nutze Fallback-BBox.")
        bbox = box(360000, 5800000, 420000, 5860000)
        return gpd.GeoDataFrame({'geometry': [bbox]}, crs=CRS)

def analyze_chunk_fast(args) -> gpd.GeoDataFrame:
    """
    analyzes a single district chunk for coverage
    1. prepares geoms (unary union)
    2. clips to district
    3. performs set operations
    4. adds planning
    5. calculates white spots
    6. returns gdf with results

    Args:
        args (tuple): (district_geom, tk_gdf, vf_gdf, plan_g)

    Returns:
        gpd.GeoDataFrame: analysis results for that district
    """
    # unpack args
    district_geom, tk_gdf, vf_gdf, plan_gdf = args

    results = []

    try:
        # 1. prepare geometries (unary union) for faster ops

        # TK
        tk_geom = unary_union(tk_gdf.geometry.values) if not tk_gdf.empty else None
        if tk_geom and tk_geom.is_empty: tk_geom = None

        # VF
        vf_geom = unary_union(vf_gdf.geometry.values) if not vf_gdf.empty else None
        if vf_geom and vf_geom.is_empty: vf_geom = None

        # Plan
        plan_geom = unary_union(plan_gdf.geometry.values) if not plan_gdf.empty else None
        if plan_geom and plan_geom.is_empty: plan_geom = None

        # 2. clip to district geom (to reduce complexity)
        if tk_geom: tk_geom = tk_geom.intersection(district_geom)
        if vf_geom: vf_geom = vf_geom.intersection(district_geom)
        if plan_geom: plan_geom = plan_geom.intersection(district_geom)

        # 3. set operations

        # Wettbewerb: TK ∩ VF
        if tk_geom and vf_geom:
            geom_comp = tk_geom.intersection(vf_geom)
            if not geom_comp.is_empty:
                results.append({'geometry': geom_comp, 'status': 'Wettbewerb', 'type': 'Bestand'})

            # Monopol TK: TK without VF
            geom_mono_tk = tk_geom.difference(vf_geom)
            if not geom_mono_tk.is_empty:
                results.append({'geometry': geom_mono_tk, 'status': 'Monopol Telekom', 'type': 'Bestand'})

            # Monopol VF: VF without TK
            geom_mono_vf = vf_geom.difference(tk_geom)
            if not geom_mono_vf.is_empty:
                results.append({'geometry': geom_mono_vf, 'status': 'Monopol Vodafone', 'type': 'Bestand'})

        elif tk_geom:
            # only TK present
            results.append({'geometry': tk_geom, 'status': 'Monopol Telekom', 'type': 'Bestand'})

        elif vf_geom:
            # only VF present
            results.append({'geometry': vf_geom, 'status': 'Monopol Vodafone', 'type': 'Bestand'})

        # 4. add planning
        if plan_geom and not plan_geom.is_empty:
             results.append({'geometry': plan_geom, 'status': 'Telekom Planung', 'type': 'Planung'})

        # 5. calc white spots (district minus all infra)

        # collect all infra geoms
        infra_list = []
        if tk_geom: infra_list.append(tk_geom)
        if vf_geom: infra_list.append(vf_geom)
        if plan_geom: infra_list.append(plan_geom)

        if infra_list:
            total_infra = unary_union(infra_list)
            white_spot_geom = district_geom.difference(total_infra)
        else:
            # no infra at all
            white_spot_geom = district_geom

        if not white_spot_geom.is_empty:
            results.append({'geometry': white_spot_geom, 'status': 'White Spot', 'type': 'Lücke'})

        # 6. return results as gdf
        if results:
            return gpd.GeoDataFrame(results, crs=CRS)
        else:
            return gpd.GeoDataFrame(columns=['geometry', 'status', 'type'], crs=CRS)

    except Exception as e:
        print(f"❌ Fehler im Worker: {e}")
        return gpd.GeoDataFrame(columns=['geometry', 'status', 'type'], crs=CRS)

def main(preloaded_data=None) -> None:
    """
    main function to run the analysis
    1. load data from preload (RAM) or disk
    2. load districts for splitting
    3. prepare tasks
    4. parallel process
    5. merge & save results

    Args:
        preloaded_data (dict, optional): preloaded data from previous step in RAM. default is none

    Returns:
        None
    """
    logging.info("🚀 Starte Analyse (RAM-Boosted)")

    # 1. load data (TK + VF + Planung)
    if preloaded_data:
        logging.info("⚡ Nutze Daten aus Arbeitsspeicher (Skip I/O)...")
        # Wir erwarten, dass preloaded_data ein Dict ist: {'tk_2000': gdf, ...}
        # Fallback auf leere GDFs falls Key fehlt
        gdf_tk_2000 = preloaded_data.get("tk_2000", gpd.GeoDataFrame(columns=['geometry'], crs=CRS))
        gdf_tk_1000 = preloaded_data.get("tk_1000", gpd.GeoDataFrame(columns=['geometry'], crs=CRS))
        gdf_tk_plan = preloaded_data.get("tk_plan", gpd.GeoDataFrame(columns=['geometry'], crs=CRS))
        gdf_vf_1000 = preloaded_data.get("vf_1000", gpd.GeoDataFrame(columns=['geometry'], crs=CRS))
    else:
        # Alter Weg (Disk)
        gdf_tk_2000 = load_layer_safe("tk_2000")
        gdf_tk_1000 = load_layer_safe("tk_1000")
        gdf_tk_plan = load_layer_safe("tk_plan")
        gdf_vf_1000 = load_layer_safe("vf_1000")

    # TK Merge im RAM
    gdf_tk_total = pd.concat([gdf_tk_2000, gdf_tk_1000], ignore_index=True)

    # 2. districts for splitting
    gdf_districts = load_districts_for_splitting()

    tasks = []
    
    # add spatial index if not present
    for gdf in [gdf_tk_total, gdf_vf_1000, gdf_tk_plan]:
        if not gdf.empty and gdf.sindex is None: pass

    # 3. prepare tasks
    logging.info(f"Verteile auf {len(gdf_districts)} Bezirke...")

    for _, district in gdf_districts.iterrows():
        geom = district.geometry
        bbox = geom.bounds

        # Slice per Spatial Index (nur relevante Daten übergeben)
        # Wir kopieren (.copy()), um Serialization-Issues zu vermeiden
        tk_slice = gdf_tk_total.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy() if not gdf_tk_total.empty else gdf_tk_total
        vf_slice = gdf_vf_1000.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy() if not gdf_vf_1000.empty else gdf_vf_1000
        plan_slice = gdf_tk_plan.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy() if not gdf_tk_plan.empty else gdf_tk_plan

        tasks.append((geom, tk_slice, vf_slice, plan_slice))

    # 4. Parallel Process
    max_workers = os.cpu_count()
    results_list = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = list(tqdm(
            executor.map(analyze_chunk_fast, tasks),
            total=len(tasks),
            desc="⚡ Analysiere",
            unit="bezirk",
            colour="magenta"
        ))
        results_list.extend(futures)

    # 5. Merge & Save
    if results_list:
        gdf_final = pd.concat(results_list, ignore_index=True)

        # Explode MultiPolygons
        gdf_final = gdf_final.explode(index_parts=False).reset_index(drop=True)

        print("\n" + "="*30)
        logging.info("📊 STATISTIK BERLIN (km²)")
        print("="*30)
        try:
            stats = gdf_final.dissolve(by='status').area / 1_000_000
            print(stats.round(2))
        except: pass
        print("="*30 + "\n")

        if os.path.exists(ANALYSIS_OUTPUT_GPKG): os.remove(ANALYSIS_OUTPUT_GPKG)
        gdf_final.to_file(ANALYSIS_OUTPUT_GPKG, layer="analyse_berlin", driver="GPKG", engine="pyogrio")
        logging.info("✅ Analyse gespeichert.")
    else:
        logging.warning("Keine Ergebnisse.")

if __name__ == "__main__":
    main()