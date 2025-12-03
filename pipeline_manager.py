import logging
import time
import os
import sys
import gc
import importlib
from datetime import timedelta

# --- WINDOWS UTF-8 FIX (CRITICAL) ---
# Das verhindert den Crash bei Emojis in der Windows Konsole
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURATION ---
HAUPTORDNER = "Glasfaser_Analyse_Project"
LOG_DATEINAME = os.path.join(HAUPTORDNER, "pipeline_run.log")

# Liste der Module (Dateinamen ohne .py)
PIPELINE_STEPS = [
    ("1. Download Phase", "s01_downloader"),
    ("2. Processing Phase", "s02_processor"),
    ("3. Cleaning Phase", "s03_cleaning"),
    ("4. Analysis Phase", "s04_analysis")
]

def setup_central_logging():
    """
    Konfiguriert den Root-Logger für alle Unterskripte.
    """
    if not os.path.exists(HAUPTORDNER):
        os.makedirs(HAUPTORDNER)

    # Altes Log löschen
    if os.path.exists(LOG_DATEINAME):
        try:
            os.remove(LOG_DATEINAME)
        except PermissionError:
            print("⚠️ Warnung: Konnte altes Logfile nicht löschen (evtl. noch offen).")

    # Logger konfigurieren
    # WICHTIG: encoding='utf-8' für das File, damit Emojis im Textfile landen
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
    """Führt einen Schritt aus."""
    logger = logging.getLogger("MANAGER")
    print("\n" + "-"*60)
    logger.info(f"🚀 STARTE: {step_pretty_name}")
    print("-"*60)
    
    start_time = time.time()
    
    try:
        # Importiert das Modul (entspricht "import s01_downloader")
        module = importlib.import_module(module_name)
        
        # Falls es schon im Speicher war, neu laden (für sauberen State)
        importlib.reload(module)
        
        # main() ausführen
        if hasattr(module, 'main'):
            # Wir fangen SystemExit ab, falls ein Skript sys.exit() nutzt
            try:
                module.main()
            except SystemExit as e:
                if e.code != 0:
                    raise e
        else:
            logger.error(f"Modul {module_name} hat keine main() Funktion!")
            return False

    except ImportError:
        logger.error(f"❌ DATEI FEHLT: {module_name}.py nicht gefunden!")
        logger.error("Hast du die Dateien umbenannt (z.B. s01_downloader.py)?")
        return False
    except Exception as e:
        logger.exception(f"❌ FEHLER in {step_pretty_name}: {e}")
        return False
    
    # Zeitmessung
    duration = time.time() - start_time
    duration_str = str(timedelta(seconds=int(duration)))
    
    logger.info(f"✅ BEENDET: {step_pretty_name}")
    logger.info(f"⏱️  Dauer Schritt: {duration_str}")
    
    # Speicher aufräumen
    gc.collect()
    return True

def main():
    # 1. Logging VOR allem anderen starten
    setup_central_logging()
    logger = logging.getLogger("MANAGER")
    
    logger.info("=== 5G INTELLIGENCE PIPELINE GESTARTET ===")
    
    total_start = time.time()
    success_count = 0
    
    # 2. Pipeline Loop
    for pretty_name, script_name in PIPELINE_STEPS:
        success = run_step(pretty_name, script_name)
        if not success:
            logger.error("🛑 PIPELINE GESTOPPT (Kritischer Fehler)")
            break
        success_count += 1
    
    # 3. Abschlussbericht
    total_end = time.time()
    total_duration = str(timedelta(seconds=int(total_end - total_start)))
    
    print("\n" + "="*60)
    if success_count == len(PIPELINE_STEPS):
        logger.info(f"🏆 GESAMT-ERFOLG! Alle Schritte durchlaufen.")
    else:
        logger.warning(f"⚠️  PIPELINE UNVOLLSTÄNDIG ({success_count}/{len(PIPELINE_STEPS)} Schritte).")
    
    logger.info(f"⏱️  Gesamtlaufzeit: {total_duration}")
    print("="*60)

if __name__ == "__main__":
    main()