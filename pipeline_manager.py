import os
from datetime import datetime
import importlib
import time
import sys
from datetime import timedelta
import config
import utils

# --- WINDOWS UTF-8 FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def should_skip_to_step_04():
    """
    Prüft, ob die Output-Dateien von Schritt 03 existieren und jünger als 1 Woche sind.
    """
    required_keys = ["clean_tk_2000", "clean_tk_1000", "clean_tk_plan", "clean_vf_1000"]
    week_ago = time.time() - (7 * 24 * 60 * 60)
    
    logger = utils.setup_logger("MANAGER", config.LOG_FILES["manager"])
    
    missing_files = []
    old_files = []
    
    for key in required_keys:
        filepath = config.GPKG_FILES[key]
        if not os.path.exists(filepath):
            missing_files.append(key)
            continue
            
        # Prüfe Alter
        mtime = os.path.getmtime(filepath)
        if mtime < week_ago:
            old_files.append(key)
            
    if missing_files:
        logger.info(f"ℹ️ Smart-Check: Dateien fehlen ({', '.join(missing_files)}). Starte von vorne.")
        return False
        
    if old_files:
        logger.info(f"ℹ️ Smart-Check: Dateien zu alt ({', '.join(old_files)}). Starte von vorne.")
        return False
        
    logger.info("ℹ️ Smart-Check: Cleaning-Dateien sind aktuell (< 7 Tage). Überspringe Schritt 1-3.")
    return True

def run_step(step_pretty_name, module_name):
    logger = utils.setup_logger("MANAGER", config.LOG_FILES["manager"])
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
    return True

def main():
    logger = utils.setup_logger("MANAGER", config.LOG_FILES["manager"])
    logger.info("=== 5G INTELLIGENCE PIPELINE GESTARTET ===")
    
    total_start = time.time()
    success_count = 0
    
    # Smart-Skip Logik
    start_index = 0
    if should_skip_to_step_04():
        # Finde Index von s04_analysis
        for i, (pname, sname) in enumerate(config.PIPELINE_STEPS):
            if sname == "s04_analysis":
                start_index = i
                break
    
    steps_to_run = config.PIPELINE_STEPS[start_index:]
    
    for pretty_name, script_name in steps_to_run:
        success = run_step(pretty_name, script_name)
        if not success:
            logger.error("🛑 PIPELINE GESTOPPT (Kritischer Fehler)")
            break
        success_count += 1
    
    total_duration = str(timedelta(seconds=int(time.time() - total_start)))
    
    print("\n" + "="*60)
    if success_count == len(steps_to_run):
        logger.info(f"🏆 GESAMT-ERFOLG! Alle geplanten Schritte durchlaufen.")
    else:
        logger.warning(f"⚠️  PIPELINE UNVOLLSTÄNDIG ({success_count}/{len(steps_to_run)} Schritte).")
    logger.info(f"⏱️  Gesamtlaufzeit: {total_duration}")
    print("="*60)

if __name__ == "__main__":
    main()