import logging
import time
import os
import sys
import gc
import importlib
from datetime import timedelta

# --- WINDOWS UTF-8 FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "pipeline_run.log")

# Hier wurde Schritt 5 ergänzt!
PIPELINE_STEPS = [
    ("1. Download Phase", "s01_downloader"),
    ("2. Processing Phase", "s02_processor"),
    ("3. Cleaning Phase", "s03_cleaning"),
    ("4. Analysis Phase", "s04_analysis"),
    ("5. Enrichment Phase", "s05_enrichment"),
    ("6. Visualization Phase", "s06_visualization")
]

def setup_central_logging():
    if not os.path.exists(HAUPTORDNER):
        os.makedirs(HAUPTORDNER)
    if os.path.exists(LOG_DATEINAME):
        try: os.remove(LOG_DATEINAME)
        except: pass

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_DATEINAME, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_step(step_pretty_name, module_name):
    logger = logging.getLogger("MANAGER")
    print("\n" + "-"*60)
    logger.info(f"🚀 STARTE: {step_pretty_name}")
    print("-"*60)
    
    start_time = time.time()
    
    try:
        module = importlib.import_module(module_name)
        importlib.reload(module)
        
        if hasattr(module, 'main'):
            try:
                module.main()
            except SystemExit as e:
                if e.code != 0: raise e
        else:
            logger.error(f"Modul {module_name} hat keine main() Funktion!")
            return False

    except ImportError:
        logger.error(f"❌ DATEI FEHLT: {module_name}.py nicht gefunden!")
        return False
    except Exception as e:
        logger.exception(f"❌ FEHLER in {step_pretty_name}: {e}")
        return False
    
    duration_str = str(timedelta(seconds=int(time.time() - start_time)))
    logger.info(f"✅ BEENDET: {step_pretty_name}")
    logger.info(f"⏱️  Dauer Schritt: {duration_str}")
    gc.collect()
    return True

def main():
    setup_central_logging()
    logger = logging.getLogger("MANAGER")
    logger.info("=== 5G INTELLIGENCE PIPELINE GESTARTET ===")
    
    total_start = time.time()
    success_count = 0
    
    for pretty_name, script_name in PIPELINE_STEPS:
        success = run_step(pretty_name, script_name)
        if not success:
            logger.error("🛑 PIPELINE GESTOPPT (Kritischer Fehler)")
            break
        success_count += 1
    
    total_duration = str(timedelta(seconds=int(time.time() - total_start)))
    
    print("\n" + "="*60)
    if success_count == len(PIPELINE_STEPS):
        logger.info(f"🏆 GESAMT-ERFOLG! Alle Schritte durchlaufen.")
    else:
        logger.warning(f"⚠️  PIPELINE UNVOLLSTÄNDIG ({success_count}/{len(PIPELINE_STEPS)} Schritte).")
    logger.info(f"⏱️  Gesamtlaufzeit: {total_duration}")
    print("="*60)

if __name__ == "__main__":
    main()