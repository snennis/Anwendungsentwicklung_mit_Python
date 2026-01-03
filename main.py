import logging
import time
import os
import sys
import gc
import importlib
import inspect
from datetime import timedelta
from config import BASE_DIR, OUTPUT_DIR, CACHE_DIR, LOG_DIR, LOG_FILE_PATH, PIPELINE_STEPS

# --- WINDOWS UTF-8 FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def setup_directory_structure():
    """Ensures that the output directory structure exists."""
    for d in [BASE_DIR, OUTPUT_DIR, CACHE_DIR, LOG_DIR]:
        if not os.path.exists(d):
            os.makedirs(d)

def setup_central_logging():
    # Setup directories first
    setup_directory_structure()
    
    # Remove old main log if exists
    if os.path.exists(LOG_FILE_PATH):
        try: os.remove(LOG_FILE_PATH)
        except: pass

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE_PATH, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_step(step_pretty_name, module_name, input_data=None):
    """
    Führt einen Pipeline-Schritt aus.
    Erweitert: Kann nun input_data übergeben und gibt (success, result) zurück.
    """
    logger = logging.getLogger("MANAGER")
    print("\n" + "-"*60)
    logger.info(f"🚀 STARTE: {step_pretty_name}")
    print("-"*60)
    
    start_time = time.time()
    result = None
    
    try:
        module = importlib.import_module(module_name)
        importlib.reload(module)
        
        if hasattr(module, 'main'):
            try:
                # Intelligente Parameter-Prüfung
                # Wir schauen, ob die main() Funktion Argumente akzeptiert
                sig = inspect.signature(module.main)
                params = sig.parameters
                
                if len(params) > 0 and input_data is not None:
                    # Modul kann Daten empfangen -> Wir füttern es (RAM-Mode)
                    logger.info(f"⚡ Übergebe In-Memory Daten an {module_name}...")
                    result = module.main(input_data)
                else:
                    # Klassischer Aufruf ohne Argumente
                    result = module.main()
                    
            except SystemExit as e:
                if e.code != 0: raise e
        else:
            logger.error(f"Modul {module_name} hat keine main() Funktion!")
            return False, None

    except ImportError as e:
        logger.error(f"❌ DATEI FEHLT: {module_name}. {e}")
        return False, None
    except Exception as e:
        logger.exception(f"❌ FEHLER in {step_pretty_name}: {e}")
        return False, None
    
    duration_str = str(timedelta(seconds=int(time.time() - start_time)))
    logger.info(f"✅ BEENDET: {step_pretty_name}")
    logger.info(f"⏱️  Dauer Schritt: {duration_str}")
    gc.collect()
    
    return True, result

def main():
    setup_central_logging()
    logger = logging.getLogger("MANAGER")
    logger.info("=== 5G INTELLIGENCE PIPELINE GESTARTET ===")
    logger.info(f"📂 Output: {OUTPUT_DIR}")
    
    total_start = time.time()
    success_count = 0
    
    # Hier speichern wir Daten im RAM, um sie zwischen Schritten zu übergeben
    pipeline_memory = None
    
    for pretty_name, script_name in PIPELINE_STEPS:
        
        # Logik: Welche Daten bekommt der aktuelle Schritt?
        current_input = None
        
        # Wenn wir bei Schritt 4 sind, geben wir ihm die Daten aus Schritt 3 (falls vorhanden)
        if "s04_analysis" in script_name and pipeline_memory is not None:
            current_input = pipeline_memory
        
        # Schritt ausführen
        success, step_result = run_step(pretty_name, script_name, input_data=current_input)
        
        if not success:
            logger.error("🛑 PIPELINE GESTOPPT (Kritischer Fehler)")
            break
        
        # Logik: Speichern wir das Ergebnis für den nächsten Schritt?
        # Wenn Schritt 3 fertig ist, merken wir uns dessen Output (Dict mit GDFs)
        if "s03_cleaning" in script_name:
            if step_result and isinstance(step_result, dict):
                count = len(step_result)
                logger.info(f"💾 Behalte {count} Layer aus Cleaning im Arbeitsspeicher.")
                pipeline_memory = step_result
            else:
                logger.warning("⚠️ Cleaning lieferte keine In-Memory Daten zurück (Fallback auf Disk I/O).")
                pipeline_memory = None

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