import logging
import time
import os
import sys
import gc
from datetime import timedelta
import importlib

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "pipeline_run.log")

# Die Module müssen im selben Ordner liegen und valide Python-Namen haben (keine Zahlen am Anfang!)
# Bitte benenne deine Dateien um: 01_downloader.py -> s01_downloader.py usw.
PIPELINE_STEPS = [
    ("Download Phase", "s01_downloader"),
    ("Processing Phase (Vectorization)", "s02_processor"),
    ("Cleaning Phase (Geometry Fix)", "s03_cleaning"),
    ("Analysis Phase (Intelligence)", "s04_analysis")
]

def setup_central_logging():
    """
    Konfiguriert den Root-Logger.
    Da alle Unterskripte logging.basicConfig() nutzen, wird deren Konfiguration 
    ignoriert, solange wir den Logger HIER zuerst initialisieren.
    """
    if not os.path.exists(HAUPTORDNER):
        os.makedirs(HAUPTORDNER)

    # Vorheriges Log löschen für sauberen Neustart
    if os.path.exists(LOG_DATEINAME):
        os.remove(LOG_DATEINAME)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_DATEINAME, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("=== PIPELINE GESTARTET ===")
    logging.info(f"Logfile: {LOG_DATEINAME}")

def run_step(step_name, module_name):
    """Führt einen Pipeline-Schritt aus und misst die Zeit."""
    logger = logging.getLogger("PIPELINE")
    logger.info("-" * 60)
    logger.info(f"🚀 STARTE: {step_name} ({module_name}.py)")
    
    start_time = time.time()
    
    try:
        # Dynamischer Import des Moduls
        # Das erlaubt uns, die Module sauber auszuführen
        module = importlib.import_module(module_name)
        
        # Falls das Modul schon geladen war (z.B. bei Tests), laden wir es neu
        importlib.reload(module)
        
        # Wir rufen die main() Funktion des Moduls auf
        if hasattr(module, 'main'):
            module.main()
        else:
            logger.error(f"Modul {module_name} hat keine main() Funktion!")
            return False

    except ImportError:
        logger.error(f"CRITICAL: Konnte {module_name}.py nicht finden! Hast du die Dateien umbenannt (s01_...)?")
        return False
    except Exception as e:
        logger.exception(f"CRITICAL: Fehler in {step_name}: {e}")
        return False
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Formatierung der Zeit
    duration_str = str(timedelta(seconds=int(duration)))
    logger.info(f"✅ BEENDET: {step_name}")
    logger.info(f"⏱️  Dauer: {duration_str}")
    
    # Speicher aufräumen (wichtig nach Processing!)
    gc.collect()
    
    return True

def main():
    setup_central_logging()
    logger = logging.getLogger("MASTER")
    
    total_start = time.time()
    success_count = 0
    
    for pretty_name, script_name in PIPELINE_STEPS:
        success = run_step(pretty_name, script_name)
        if not success:
            logger.error("🛑 PIPELINE ABGEBROCHEN WEGEN FEHLER.")
            break
        success_count += 1
        
    total_end = time.time()
    total_duration = str(timedelta(seconds=int(total_end - total_start)))
    
    logger.info("=" * 60)
    if success_count == len(PIPELINE_STEPS):
        logger.info(f"🎉 GESAMT-PIPELINE ERFOLGREICH ABGESCHLOSSEN")
    else:
        logger.warning(f"⚠️  PIPELINE UNVOLLSTÄNDIG ({success_count}/{len(PIPELINE_STEPS)} Schritte)")
        
    logger.info(f"⏱️  Gesamtlaufzeit: {total_duration}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()