import os
import geopandas as gpd
import pandas as pd
import osmnx as ox
import logging
from shapely.geometry import box

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "03_cleaning.log")
ANALYSIS_CRS = "EPSG:25833" # Metrisches System

# Input -> Output Mapping
LAYERS_TO_CLEAN = [
    {"input_dir": "output_parquet/tk_2000", "output": "clean_tk_2000.parquet", "radius": 7.0, "name": "Telekom 2000"},
    {"input_dir": "output_parquet/tk_1000", "output": "clean_tk_1000.parquet", "radius": 7.0, "name": "Telekom 1000"},
    {"input_dir": "output_parquet/tk_plan", "output": "clean_tk_plan.parquet", "radius": 7.0, "name": "Telekom Plan"},
    {"input_dir": "output_parquet/vf_1000", "output": "clean_vf_1000.parquet", "radius": 3.0, "name": "Vodafone 1000"}
]

# Cache für die Grenze, damit wir sie nicht 4x laden müssen
_BERLIN_BOUNDARY_CACHE = None

def get_city_shape(city: str):
    """Lädt die exakte Grenze von Berlin (ohne Brandenburg)."""
    global _BERLIN_BOUNDARY_CACHE
    if _BERLIN_BOUNDARY_CACHE is not None:
        return _BERLIN_BOUNDARY_CACHE

    print("   🏙️ Lade Berlin-Grenze für Clipping...")
    try:
        # Lade Berlin
        gdf = ox.geocode_to_gdf(city)
        # Reprojizieren
        gdf = gdf.to_crs(ANALYSIS_CRS)
        # Dissolve (falls mehrere Teile)
        _BERLIN_BOUNDARY_CACHE = gdf.dissolve().geometry.iloc[0]
        return _BERLIN_BOUNDARY_CACHE
    except Exception as e:
        print(f"   ⚠️ Fehler beim Laden der Grenze: {e}")
        # Fallback BBox (Ungefähr Berlin Mitte)
        return box(360000, 5800000, 420000, 5860000) # Grobe UTM33 Koordinaten

def clean_geometry_layer(config):
    in_dir = os.path.join(HAUPTORDNER, config["input_dir"])
    out_path = os.path.join(HAUPTORDNER, config["output"])
    radius = config["radius"]
    
    if not os.path.exists(in_dir):
        print(f"⚠️ Input Directory fehlt: {in_dir}")
        return

    print(f"🧹 Verarbeite {config['name']}...")
    
    try:
        # 1. Laden (Parquet Partitions)
        # GeoPandas read_parquet kann Verzeichnisse manchmal nicht direkt lesen,
        # daher lesen wir explizit alle *.parquet files
        parquet_files = [os.path.join(in_dir, f) for f in os.listdir(in_dir) if f.endswith('.parquet')]
        
        if not parquet_files:
            print(f"   ⚠️ Keine Parquet-Dateien in {in_dir}")
            return
            
        dfs = []
        for pf in parquet_files:
            try:
                # Schnelles Laden ohne komplette Validierung
                dfs.append(gpd.read_parquet(pf))
            except Exception as e:
                print(f"   ⚠️ Fehler beim Lesen von {pf}: {e}")
        
        if not dfs:
            return

        gdf = pd.concat(dfs, ignore_index=True)
        
        if gdf.empty:
            print(f"   ⚠️ Leer.")
            return

        # 2. Reprojektion
        if gdf.crs != ANALYSIS_CRS:
            gdf = gdf.to_crs(ANALYSIS_CRS)

        # 3. HARD CLIPPING (Alles außerhalb von Berlin abschneiden)
        berlin_shape = get_city_shape("Berlin, Germany")
        
        print(f"   ✂️ Schneide auf Stadtgrenze zu ({len(gdf)} Features)...")
        gdf = gdf.clip(berlin_shape)
        
        if gdf.empty:
            print(f"   ⚠️ Nach Clipping leer (keine Daten in Berlin).")
            return

        print(f"   🔧 Repariere Korridore (Radius: {radius}m)...")
        
        # 4. Cleaning (Buffer-Trick)
        # Buffer operation kann teuer sein, aber notwendig für 'Lückenfüllung'
        gdf['geometry'] = gdf.geometry.buffer(radius, resolution=3)
        gdf = gdf.dissolve()
        gdf['geometry'] = gdf.geometry.buffer(-radius, resolution=3)
        gdf['geometry'] = gdf.geometry.buffer(0)
        
        # 5. Speichern
        gdf.to_parquet(out_path, compression='snappy')
        print(f"   ✅ Fertig: {config['output']}")
        
    except Exception as e:
        logging.error(f"Fehler bei {config['name']}: {e}")
        print(f"   ❌ Fehler: {e}")

def main():
    if not os.path.exists(HAUPTORDNER): return
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_DATEINAME, mode='w')])
    
    print("🚀 Starte Geometrie-Cleaning & Clipping")
    for layer in LAYERS_TO_CLEAN:
        clean_geometry_layer(layer)
    print("\n✨ Fertig.")

if __name__ == "__main__":
    main()