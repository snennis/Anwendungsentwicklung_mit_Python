import os
import geopandas as gpd
import logging
from tqdm import tqdm

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "03_cleaning.log")
ANALYSIS_CRS = "EPSG:25833" # Metrisches System (Berlin/BB) für korrektes Buffering

# Input -> Output Mapping mit Reinigungs-Radius
# Radius = Halbe Breite der Lücke, die geschlossen werden soll + Puffer
LAYERS_TO_CLEAN = [
    {
        "input": "raw_tk_2000.gpkg", 
        "output": "clean_tk_2000.gpkg",
        # Ziel: 11.5m Löcher & 12m Korridore schließen.
        # 12m / 2 = 6m. Wir nehmen 7.0m zur Sicherheit.
        "radius": 7.0, 
        "name": "Telekom 2000"
    },
    {
        "input": "raw_tk_1000.gpkg", 
        "output": "clean_tk_1000.gpkg",
        # Ziel: 12m Korridore schließen.
        "radius": 7.0,
        "name": "Telekom 1000"
    },
    {
        "input": "raw_tk_plan.gpkg", 
        "output": "clean_tk_plan.gpkg",
        # Planungsdaten sind oft fragmentiert. 7m ist konsistent.
        "radius": 7.0,
        "name": "Telekom Plan"
    },
    {
        "input": "raw_vf_1000.gpkg", 
        "output": "clean_vf_1000.gpkg",
        # Ziel: 4m Korridore schließen.
        # 4m / 2 = 2m. Wir nehmen 3.0m.
        "radius": 3.0,
        "name": "Vodafone 1000"
    }
]

def clean_geometry_layer(config):
    in_path = os.path.join(HAUPTORDNER, config["input"])
    out_path = os.path.join(HAUPTORDNER, config["output"])
    radius = config["radius"]
    
    if not os.path.exists(in_path):
        logging.warning(f"Input fehlt: {config['input']}")
        return

    print(f"🧹 Reinige {config['name']} (Radius: {radius}m)...")
    
    try:
        # 1. Laden
        gdf = gpd.read_file(in_path)
        if gdf.empty:
            print(f"   ⚠️ Leer.")
            return

        # 2. Reprojektion (Zwingend für Meter-Buffer)
        if gdf.crs != ANALYSIS_CRS:
            gdf = gdf.to_crs(ANALYSIS_CRS)

        print(f"   -> Repariere {len(gdf)} Fragmente...")
        
        # 3. Buffer-Trick (Closing Gap)
        # Positiver Buffer -> Verschmilzt Teile und schließt Lücken
        # resolution=3 reicht hier, da wir Kanten glätten wollen
        gdf['geometry'] = gdf.geometry.buffer(radius, resolution=3)
        
        # 4. Dissolve (Verschmelzen zu riesigen Multi-Polygonen)
        # Das ist der teure Schritt!
        gdf = gdf.dissolve()
        
        # 5. Negativer Buffer (Form wiederherstellen)
        # Zieht die Außenkanten zurück, aber innere Löcher bleiben zu
        gdf['geometry'] = gdf.geometry.buffer(-radius, resolution=3)
        
        # 6. Aufräumen
        gdf['geometry'] = gdf.geometry.buffer(0) # Fix Topology
        
        # 7. Speichern
        gdf.to_file(out_path, driver="GPKG")
        print(f"   ✅ Fertig! Gespeichert als {config['output']}")
        
    except Exception as e:
        logging.error(f"Fehler bei {config['name']}: {e}")
        print(f"   ❌ Fehler: {e}")

def main():
    if not os.path.exists(HAUPTORDNER): return
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_DATEINAME, mode='w')])
    
    print("🚀 Starte Geometrie-Cleaning (Korridore & Löcher schließen)")
    
    for layer in LAYERS_TO_CLEAN:
        clean_geometry_layer(layer)
        
    print("\n✨ Cleaning abgeschlossen. Weiter zu Schritt 04.")

if __name__ == "__main__":
    main()