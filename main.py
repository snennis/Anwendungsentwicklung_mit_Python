"""
This is the main manager script for the fiber optic analysis pipeline. It orchestrates the execution of various pipeline steps.
Each step is defined in a separate module. It sets up logging, manages directory structures and handles errors.
"""
import logging
import time
import os
import sys
import gc # garbage collector interface -> to free up memory
import importlib
import inspect
from datetime import timedelta
from config import BASE_DIR, OUTPUT_DIR, CACHE_DIR, LOG_DIR, LOG_FILE_PATH, PIPELINE_STEPS

# windows UTF-8 fix
# ensures proper UTF-8 output in windows consoles
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def setup_directory_structure() -> None:
    """
    creates necessary directories if they do not exist

    Returns:
        None
    """
    # comes from config.py
    for d in [BASE_DIR, OUTPUT_DIR, CACHE_DIR, LOG_DIR]:
        if not os.path.exists(d):
            os.makedirs(d)

def setup_central_logging() -> None:
    """
    sets up central logging for the pipeline:
        1. removes old log file if exist
        2. configures logging to file and console

    Returns:
        None
    """
    # Setup directories first
    setup_directory_structure()
    
    # Remove old main log if exists
    if os.path.exists(LOG_FILE_PATH):
        try:
            os.remove(LOG_FILE_PATH)
        except:
            pass

    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE_PATH, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_step(step_pretty_name: str, module_name: str, input_data=None) -> tuple[bool, any]:
    """
    runs a pipeline step by importing its module and calling its main() function

    Args:
        step_pretty_name (str): human-readable name of the step for logging. comes from config.py PIPELINE_STEPS
        module_name (str): module name to import and run (e.g. "steps.s01_downloader")
        input_data (any, optional): data to pass to the steps main() function (if it accepts parameters)

    Returns:
        tuple[bool, any]: success flag and optional result from step
    """
    # start logging
    logger = logging.getLogger("MANAGER")
    print("\n" + "-"*60)
    logger.info(f"🚀 STARTE: {step_pretty_name}")
    print("-"*60)

    # track duration
    start_time = time.time()
    result = None

    # import and run the module
    try:
        module = importlib.import_module(module_name)
        importlib.reload(module)
        
        if hasattr(module, 'main'):
            try:
                # check if main() accepts parameters
                sig = inspect.signature(module.main)
                params = sig.parameters

                # call main() with or without input_data
                if len(params) > 0 and input_data is not None:
                    # module can accept input data
                    logger.info(f"⚡ Übergebe In-Memory Daten an {module_name}...")
                    result = module.main(input_data)

                else:
                    # call without parameters
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

def main() -> None:
    """
    main function to run the entire pipeline step by step
    """
    setup_central_logging()
    logger = logging.getLogger("MANAGER")
    logger.info("=== 5G INTELLIGENCE PIPELINE GESTARTET ===")
    logger.info(f"📂 Output: {OUTPUT_DIR}")
    
    total_start = time.time()
    success_count = 0
    
    # logic: we save data from one step in memory to pass to the next step
    pipeline_memory = None
    
    for pretty_name, script_name in PIPELINE_STEPS:
        
        # logic: do we have in-memory data to pass to this step?
        current_input = None
        
        # only step s04_analysis accepts in-memory data from s03_cleaning
        if "s04_analysis" in script_name and pipeline_memory is not None:
            current_input = pipeline_memory
        
        # run step
        success, step_result = run_step(pretty_name, script_name, input_data=current_input)
        
        if not success:
            logger.error("🛑 PIPELINE GESTOPPT (Kritischer Fehler)")
            break
        
        # logic: only s03_cleaning returns data to keep in memory for next step
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