import os
from dataclasses import dataclass
from typing import List, Tuple, Dict, Any

# --- GLOBAL SETTINGS ---
BASE_DIR = "fiber_data"
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CACHE_DIR = os.path.join(BASE_DIR, "cache")
LOG_DIR = os.path.join(BASE_DIR, "logs")

CRS = "EPSG:25833"  # UTM 33N (Berlin Standard)

# --- LOGGING CONFIG ---
LOG_FILE_PATH = os.path.join(LOG_DIR, "pipeline_full.log")

def get_log_path(filename: str = None) -> str:
    """Returns the central log file path."""
    return LOG_FILE_PATH

# --- S01 DOWNLOADER CONFIG ---
ANALYSE_BBOX = {
    "X_START": 1450000.0,
    "Y_START": 6940000.0,
    "X_ENDE": 1540000.0,
    "Y_ENDE": 6840000.0
}

DOWNLOAD_MAX_WORKERS = os.cpu_count()

@dataclass
class LayerConfig:
    name: str
    service_type: str
    base_url: str
    layers_param: str
    kachel_breite_meter: float
    kachel_hoehe_meter: float
    pixel_width: int
    pixel_height: int
    subdir: str
    dpi: float = 96
    bboxSR: str = "3857"
    imageSR: str = "3857"

# Grid Parameter (Optimized)
tk_res = (1488381.81 - 1487158.82) / 256.0
tk_dim = 2048 * tk_res
vf_res = (1489909.16 - 1487728.32) / 1506.0
vf_w = 3000 * vf_res
vf_h = int(3000 * (1793/1506)) * vf_res

DOWNLOAD_LAYERS = [
    LayerConfig("Telekom_Fiber_Total", "wms", "https://t-map.telekom.de/tmap2/geoserver/public/tmap/public/wms", 
               "public:coverage_fixedline_fiber", tk_dim, tk_dim, 2048, 2048, os.path.join(CACHE_DIR, "tiles_tk_fiber")),
    LayerConfig("Vodafone_Fiber_Total", "arcgis", "https://netmap.vodafone.de/arcgis/rest/services/CoKart/netzabdeckung_fixnet_4x/MapServer/export", 
               "show:3", vf_w, vf_h, 3000, int(3000 * (1793/1506)), os.path.join(CACHE_DIR, "tiles_vf_fiber"), dpi=158.4, bboxSR="102100", imageSR="102100")
]

# --- S02 PROCESSOR CONFIG ---
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

PROCESSING_LAYERS = [
    ProcessConfig("Telekom Fiber", os.path.join(CACHE_DIR, "tiles_tk_fiber"), [
        ExtractionRule("2000 Mbit", hex_to_rgba("#610332"), os.path.join(CACHE_DIR, "raw_tk_2000.gpkg"), "2000"),
        ExtractionRule("1000 Mbit", hex_to_rgba("#7D4443"), os.path.join(CACHE_DIR, "raw_tk_1000.gpkg"), "1000"),
        ExtractionRule("Geplant", hex_to_rgba("#314EA5"), os.path.join(CACHE_DIR, "raw_tk_plan.gpkg"), "Plan"),
    ]),
    ProcessConfig("Vodafone Fiber", os.path.join(CACHE_DIR, "tiles_vf_fiber"), [
        ExtractionRule("1000 Mbit", hex_to_rgba("#7F0000"), os.path.join(CACHE_DIR, "raw_vf_1000.gpkg"), "1000")
    ])
]

# --- S03 CLEANING CONFIG ---
CLEANING_LAYERS = [
    {"input": os.path.join(CACHE_DIR, "raw_tk_2000.gpkg"), "output": os.path.join(CACHE_DIR, "clean_tk_2000.gpkg"), "radius": 7.0, "name": "Telekom 2000"},
    {"input": os.path.join(CACHE_DIR, "raw_tk_1000.gpkg"), "output": os.path.join(CACHE_DIR, "clean_tk_1000.gpkg"), "radius": 7.0, "name": "Telekom 1000"},
    {"input": os.path.join(CACHE_DIR, "raw_tk_plan.gpkg"), "output": os.path.join(CACHE_DIR, "clean_tk_plan.gpkg"), "radius": 7.0, "name": "Telekom Plan"},
    {"input": os.path.join(CACHE_DIR, "raw_vf_1000.gpkg"), "output": os.path.join(CACHE_DIR, "clean_vf_1000.gpkg"), "radius": 3.0, "name": "Vodafone 1000"}
]

# --- S04 ANALYSIS CONFIG ---
ANALYSIS_INPUT_FILES = {
    "tk_2000": os.path.join(CACHE_DIR, "clean_tk_2000.gpkg"),
    "tk_1000": os.path.join(CACHE_DIR, "clean_tk_1000.gpkg"),
    "tk_plan": os.path.join(CACHE_DIR, "clean_tk_plan.gpkg"),
    "vf_1000": os.path.join(CACHE_DIR, "clean_vf_1000.gpkg")
}
ANALYSIS_OUTPUT_GPKG = os.path.join(CACHE_DIR, "04_analysis_merged.gpkg")

# --- S05 ENRICHMENT CONFIG ---
ENRICHMENT_INPUT_GPKG = ANALYSIS_OUTPUT_GPKG
# Final result goes to OUTPUT_DIR
ENRICHMENT_OUTPUT_GPKG = os.path.join(OUTPUT_DIR, "05_master_analysis.gpkg")

WFS_URLS = {
    "BEZIRKE": "https://gdi.berlin.de/services/wfs/alkis_bezirke?service=wfs&version=2.0.0&request=GetFeature&typeNames=alkis_bezirke:bezirksgrenzen&outputFormat=application/json&srsName=EPSG:25833",
    "ISU5": "https://gdi.berlin.de/services/wfs/ua_flaechennutzung?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ua_flaechennutzung:d_reale_nutzung_vegetationsbedeckung_2023&OUTPUTFORMAT=application/json"
}

# --- S06 VISUALIZATION CONFIG ---
VISUALIZATION_INPUT_GPKG = ENRICHMENT_OUTPUT_GPKG
VISUALIZATION_MAP_PNG = os.path.join(OUTPUT_DIR, "berlin_strategie_karte.png")
VISUALIZATION_MAP_HTML = os.path.join(OUTPUT_DIR, "berlin_interaktiv.html")

VISUALIZATION_COLORS = {
    "Wettbewerb": "#228B22",       # Forest Green (Alles super)
    "Telekom": "#E20074",          # Telekom Magenta
    "Vodafone": "#E60000",         # Vodafone Rot
    "Geplant": "#1E90FF",          # Dodger Blue
    "Lücke (White Spot)": "#FF8C00",# Dark Orange (Warnung!)
    "Sonstiges": "#D3D3D3"         # Light Grey (Hintergrund)
}

# --- PIPELINE STEPS ---
PIPELINE_STEPS = [
    ("1. Download Phase", "steps.s01_downloader"),
    ("2. Processing Phase", "steps.s02_processor"),
    ("3. Cleaning Phase", "steps.s03_cleaning"),
    ("4. Analysis Phase", "steps.s04_analysis"),
    ("5. Enrichment Phase", "steps.s05_enrichment"),
    ("6. Visualization Phase", "steps.s06_visualization")
]
