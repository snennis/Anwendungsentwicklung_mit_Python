import os
import geopandas as gpd
import pandas as pd
import logging
import warnings
from shapely.geometry import box
from shapely.ops import unary_union
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from config import BASE_DIR, CRS, ANALYSIS_INPUT_FILES, ANALYSIS_OUTPUT_GPKG, WFS_URLS

# Warnungen unterdrücken
warnings.filterwarnings("ignore")

def load_layer_safe(key: str) -> gpd.GeoDataFrame:
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

def load_districts_for_splitting():
    logging.info("Lade Bezirksgrenzen für Partitionierung...")
    try:
        gdf = gpd.read_file(WFS_URLS["BEZIRKE"])
        if gdf.crs != CRS:
            gdf = gdf.to_crs(CRS)
        return gdf
    except Exception as e:
        logging.warning(f"Konnte Bezirke nicht laden ({e}). Nutze Fallback-BBox.")
        bbox = box(360000, 5800000, 420000, 5860000)
        return gpd.GeoDataFrame({'geometry': [bbox]}, crs=CRS)

def analyze_chunk_fast(args):
    """
    High-Performance Worker: Nutzt Shapely Algebra statt GeoPandas Overlay.
    """
    district_geom, tk_gdf, vf_gdf, plan_gdf = args
    
    results = []
    
    try:
        # 1. Geometrien vorbereiten (Unary Union = extrem schnell)
        # Wir verschmelzen alle Polygone des Providers zu einem einzigen (Multi)Polygon
        # Das macht die Mengenlehre (A minus B) trivial.
        
        # TK
        tk_geom = unary_union(tk_gdf.geometry.values) if not tk_gdf.empty else None
        if tk_geom and tk_geom.is_empty: tk_geom = None
            
        # VF
        vf_geom = unary_union(vf_gdf.geometry.values) if not vf_gdf.empty else None
        if vf_geom and vf_geom.is_empty: vf_geom = None

        # Plan
        plan_geom = unary_union(plan_gdf.geometry.values) if not plan_gdf.empty else None
        if plan_geom and plan_geom.is_empty: plan_geom = None

        # 2. Clipping am Bezirk (Shapely Intersection)
        # Wir schneiden alles, was über den Bezirk hinausragt, ab.
        if tk_geom: tk_geom = tk_geom.intersection(district_geom)
        if vf_geom: vf_geom = vf_geom.intersection(district_geom)
        if plan_geom: plan_geom = plan_geom.intersection(district_geom)

        # 3. Mengenlehre (Pure Shapely Logic)
        
        # Wettbewerb: TK und VF überschneiden sich
        if tk_geom and vf_geom:
            geom_comp = tk_geom.intersection(vf_geom)
            if not geom_comp.is_empty:
                results.append({'geometry': geom_comp, 'status': 'Wettbewerb', 'type': 'Bestand'})
            
            # Monopol TK: TK ohne VF
            geom_mono_tk = tk_geom.difference(vf_geom)
            if not geom_mono_tk.is_empty:
                results.append({'geometry': geom_mono_tk, 'status': 'Monopol Telekom', 'type': 'Bestand'})
                
            # Monopol VF: VF ohne TK
            geom_mono_vf = vf_geom.difference(tk_geom)
            if not geom_mono_vf.is_empty:
                results.append({'geometry': geom_mono_vf, 'status': 'Monopol Vodafone', 'type': 'Bestand'})
        
        elif tk_geom:
            # Nur TK da
            results.append({'geometry': tk_geom, 'status': 'Monopol Telekom', 'type': 'Bestand'})
            
        elif vf_geom:
            # Nur VF da
            results.append({'geometry': vf_geom, 'status': 'Monopol Vodafone', 'type': 'Bestand'})

        # 4. Planung hinzufügen
        if plan_geom and not plan_geom.is_empty:
             results.append({'geometry': plan_geom, 'status': 'Telekom Planung', 'type': 'Planung'})

        # 5. White Spots berechnen
        # White Spot = Bezirk MINUS (TK u VF u Plan)
        
        # Sammle alle Infrastrukturen
        infra_list = []
        if tk_geom: infra_list.append(tk_geom)
        if vf_geom: infra_list.append(vf_geom)
        if plan_geom: infra_list.append(plan_geom)
        
        if infra_list:
            total_infra = unary_union(infra_list)
            white_spot_geom = district_geom.difference(total_infra)
        else:
            # Keine Infra im Bezirk -> Alles ist White Spot
            white_spot_geom = district_geom
            
        if not white_spot_geom.is_empty:
            results.append({'geometry': white_spot_geom, 'status': 'White Spot', 'type': 'Lücke'})

        # 6. Rückgabe als DataFrame
        if results:
            return gpd.GeoDataFrame(results, crs=CRS)
        else:
            return gpd.GeoDataFrame(columns=['geometry', 'status', 'type'], crs=CRS)

    except Exception as e:
        print(f"❌ Fehler im Worker: {e}")
        return gpd.GeoDataFrame(columns=['geometry', 'status', 'type'], crs=CRS)

def main(preloaded_data=None):
    logging.info("🚀 Starte Analyse (RAM-Boosted)")

    # 1. LADEN
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
    
    # 2. Bezirke für Partitionierung
    gdf_districts = load_districts_for_splitting()
    
    tasks = []
    
    # Spatial Index aufbauen (falls noch nicht da) für schnellen Zugriff
    for gdf in [gdf_tk_total, gdf_vf_1000, gdf_tk_plan]:
        if not gdf.empty and gdf.sindex is None: pass

    # 3. Aufgaben vorbereiten
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
        
        # Explode MultiPolygons für sauberes Output-Format
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