import os
import sys
import glob
import logging
from typing import List, Tuple
from dataclasses import dataclass
from tqdm import tqdm
import numpy as np
from scipy.ndimage import binary_closing

# --- 1. ENVIRONMENT FIX (Muss VOR rasterio Import passieren) ---
# Wir suchen den Pfad, wo PROJ im aktuellen Python Environment liegt
try:
    # Versuch, den Pfad dynamisch zu finden (funktioniert meist in venv/conda)
    base_prefix = sys.prefix
    possible_proj_paths = [
        os.path.join(base_prefix, 'Library', 'share', 'proj'), # Windows Conda
        os.path.join(base_prefix, 'share', 'proj'), # Linux/Mac
        os.path.join(base_prefix, 'lib', 'site-packages', 'rasterio', 'proj_data'), # Rasterio Wheels
        os.path.join(base_prefix, 'Lib', 'site-packages', 'rasterio', 'proj_data') # Windows Pip
    ]
    
    found_proj = False
    for p in possible_proj_paths:
        if os.path.exists(p):
            os.environ['PROJ_LIB'] = p
            print(f"🔧 PROJ_LIB repariert: Setze auf {p}")
            found_proj = True
            break
            
    if not found_proj:
        print("⚠️ WARNUNG: Konnte PROJ-Pfad nicht automatisch finden. Falls der 'PROJ Error' bleibt, setze os.environ['PROJ_LIB'] manuell im Skript.")

except Exception as e:
    print(f"Fehler beim Environment-Fix: {e}")

# JETZT erst Imports
import rasterio
from rasterio.features import shapes as rasterio_shapes
import geopandas
from shapely.geometry import shape as shapely_shape
try:
    from scipy.ndimage import binary_closing
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "processing.log")

@dataclass
class ExtractionRule:
    name: str
    color_rgba: Tuple[int, int, int, int]
    output_gpkg: str
    layer_name: str

@dataclass
class ProcessConfig:
    name: str
    subdir: str
    rules: List[ExtractionRule]

def hex_to_rgba(hex_code: str) -> Tuple[int, int, int, int]:
    hex_code = hex_code.lstrip('#')
    rgb = tuple(int(hex_code[i:i+2], 16) for i in (0, 2, 4))
    return (*rgb, 255)

def process_single_file(filepath: str, rule: ExtractionRule) -> List[dict]:
    """Verarbeitet EINE EINZIGE Kachel für EINE Farbe und gibt Vektoren zurück."""
    features = []
    try:
        with rasterio.open(filepath) as src:
            # Lese Daten (Nur kleine Kachel im RAM)
            rgba = src.read((1,2,3,4))
            transform = src.transform
            
            target = rule.color_rgba
            # Schnelle Maskierung mit NumPy (True wo Farbe passt, False sonst)
            mask = np.logical_and.reduce((
                rgba[0] == target[0], 
                rgba[1] == target[1],
                rgba[2] == target[2], 
                rgba[3] > 100
            ))

            # --- NEU: Morphological Closing zum Schließen von Lücken ---
            if np.sum(mask) > 0:
                # Wir definieren den "Pinsel". Eine 3x3 Matrix aus Einsen.
                # Das schließt Lücken von 1-2 Pixeln Breite.
                # Für größere Lücken (deine 11m Quadrate), erhöhen wir iterations.
                structure = np.ones((3, 3))
                
                # iterations=2 macht den Prozess aggressiver. Probier mal 1, 2 oder 3.
                # Das sollte für die Linie (<2m) und die Quadrate (~11m) reichen,
                # da ein Pixel bei Telekom ~4.7m sind. 2 Iterationen schließen ~2-4 Pixel.
                mask_closed = binary_closing(mask, structure=structure, iterations=2)
                
                # Konvertiere zurück zu uint8 für rasterio
                mask_final = mask_closed.astype(rasterio.uint8)
            else:
                mask_final = mask.astype(rasterio.uint8)
            # --- ENDE NEU ---
            
            if np.sum(mask_final) > 0:
                # Vektorisierung der GESCHLOSSENEN Maske
                shapes = rasterio_shapes(mask_final, mask=mask_final, transform=transform)
                for geom, val in shapes:
                    if val == 1:
                        # Wichtig: Wir speichern das Shape sofort als Shapely Object
                        features.append({
                            'geometry': shapely_shape(geom), 
                            'category': rule.name
                        })
    except Exception as e:
        # Logging minimal halten, sonst flutet es bei kaputten Files
        print(f"Fehler in {filepath}: {e}") # Zum Debuggen einkommentieren
        pass
        
    return features

def process_layer_stream(config: ProcessConfig):
    """
    Geht Datei für Datei durch (KEIN Mosaik im RAM!)
    """
    tile_dir = os.path.join(HAUPTORDNER, config.subdir)
    files = glob.glob(os.path.join(tile_dir, "*.png"))
    
    # Filtere nur Files mit .pgw
    valid_files = [f for f in files if os.path.exists(f.replace(".png", ".pgw"))]
    
    print(f"⚙️ Verarbeite {config.name}: {len(valid_files)} Kacheln gefunden.")
    
    for rule in config.rules:
        print(f"  -> Extrahiere Kategorie: {rule.name}")
        
        all_features_for_rule = []
        
        # tqdm für Fortschrittsbalken pro Farbe
        for fp in tqdm(valid_files, desc=f"Vektorisierung {rule.name}", unit="kachel", colour="blue"):
            tile_feats = process_single_file(fp, rule)
            all_features_for_rule.extend(tile_feats)
            
        # Erst JETZT erstellen wir das GeoDataFrame (nur Vektoren im RAM, keine Rasterbilder)
        if all_features_for_rule:
            print(f"     💾 Speichere {len(all_features_for_rule)} Objekte...")
            out_path = os.path.join(HAUPTORDNER, rule.output_gpkg)
            
            gdf = geopandas.GeoDataFrame(all_features_for_rule, crs="EPSG:3857")
            
            # Optional: Geometrien bereinigen (buffer(0) fixt oft Self-Intersections durchs Kacheln)
            # gdf['geometry'] = gdf.geometry.buffer(0) 
            
            gdf.to_file(out_path, driver="GPKG", layer=rule.layer_name)
            print(f"     ✅ Gespeichert in {rule.output_gpkg}")
            
            # RAM freigeben
            del gdf
            del all_features_for_rule
        else:
            print("     ℹ️ Keine Daten gefunden.")

def main():
    if not os.path.exists(HAUPTORDNER): 
        print(f"Fehler: Ordner {HAUPTORDNER} existiert nicht. Erst Download ausführen!")
        return

    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_DATEINAME, mode='w')])
    
    # Konfiguration (Muss exakt zum Downloader passen)
    LAYERS = [
        ProcessConfig(
            name="Telekom Fiber",
            subdir="tiles_tk_fiber",
            rules=[
                ExtractionRule("2000 Mbit", hex_to_rgba("#610332"), "tk_fiber_2000.gpkg", "2000"),
                ExtractionRule("1000 Mbit", hex_to_rgba("#7D4443"), "tk_fiber_1000.gpkg", "1000"),
                ExtractionRule("Geplant", hex_to_rgba("#314EA5"), "tk_fiber_plan.gpkg", "Plan"),
            ]
        ),
        ProcessConfig(
            name="Vodafone Fiber",
            subdir="tiles_vf_fiber",
            rules=[
                ExtractionRule("1000 Mbit", hex_to_rgba("#7F0000"), "vf_fiber_1000.gpkg", "1000")
            ]
        )
    ]

    for layer in LAYERS:
        process_layer_stream(layer)

    print("\n✅ Verarbeitung abgeschlossen.")

if __name__ == "__main__":
    main()