import os
import sys
import logging
import time
from functools import wraps
from datetime import timedelta
import geopandas as gpd
import osmnx as ox
from shapely.geometry import box
import config

def setup_logger(name, log_file):
    """
    Richtet einen Logger ein, der sowohl in die Datei als auch auf die Konsole schreibt.
    """
    if not os.path.exists(config.HAUPTORDNER):
        os.makedirs(config.HAUPTORDNER)
        
    formatter = logging.Formatter('%(asctime)s | %(name)-10s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')
    
    # File Handler
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Stream Handler (Stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Handler nur hinzufügen, wenn sie noch nicht existieren (Vermeidung von Duplikaten bei Reloads)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
    return logger

def timer(func):
    """Decorator zur Messung der Ausführungszeit einer Funktion."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        duration = str(timedelta(seconds=int(end - start)))
        logging.getLogger(func.__module__).info(f"⏱️  Dauer '{func.__name__}': {duration}")
        return result
    return wrapper

# Cache für Berlin-Grenze
_BERLIN_BOUNDARY_CACHE = None

def get_berlin_boundary():
    """Läd die Grenze von Berlin (gecached) als GeoDataFrame."""
    global _BERLIN_BOUNDARY_CACHE
    if _BERLIN_BOUNDARY_CACHE is not None:
        return _BERLIN_BOUNDARY_CACHE
        
    logger = logging.getLogger("utils")
    logger.info("🏙️ Lade Berlin-Grenze für Clipping...")
    try:
        gdf = ox.geocode_to_gdf("Berlin, Germany")
        gdf = gdf.to_crs(config.ANALYSIS_CRS)
        # Wir wollen nur die Geometrie, aber als GDF behalten
        # dissolve() gibt ein GDF zurück.
        _BERLIN_BOUNDARY_CACHE = gdf.dissolve()
        return _BERLIN_BOUNDARY_CACHE
    except Exception as e:
        logger.warning(f"⚠️ OSM Fehler: {e}. Nutze BBox Fallback.")
        # Fallback als GeoDataFrame
        b = box(360000, 5800000, 420000, 5860000)
        _BERLIN_BOUNDARY_CACHE = gpd.GeoDataFrame({'geometry': [b]}, crs=config.ANALYSIS_CRS)
        return _BERLIN_BOUNDARY_CACHE
