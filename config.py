import os

# --- PFADE & DATEIEN ---
HAUPTORDNER = "Glasfaser_Analyse_Project"

# Log-Dateien
LOG_FILES = {
    "manager": os.path.join(HAUPTORDNER, "pipeline_run.log"),
    "s01": os.path.join(HAUPTORDNER, "download.log"),
    "s02": os.path.join(HAUPTORDNER, "02_processing.log"),
    "s03": os.path.join(HAUPTORDNER, "03_cleaning.log"),
    "s04": os.path.join(HAUPTORDNER, "04_analysis.log"),
    "s05": os.path.join(HAUPTORDNER, "05_enrichment.log"),
    "s06": os.path.join(HAUPTORDNER, "06_visualization.log")
}

# GeoPackages
GPKG_FILES = {
    "raw_tk_2000": os.path.join(HAUPTORDNER, "raw_tk_2000.gpkg"),
    "raw_tk_1000": os.path.join(HAUPTORDNER, "raw_tk_1000.gpkg"),
    "raw_tk_plan": os.path.join(HAUPTORDNER, "raw_tk_plan.gpkg"),
    "raw_vf_1000": os.path.join(HAUPTORDNER, "raw_vf_1000.gpkg"),
    "clean_tk_2000": os.path.join(HAUPTORDNER, "clean_tk_2000.gpkg"),
    "clean_tk_1000": os.path.join(HAUPTORDNER, "clean_tk_1000.gpkg"),
    "clean_tk_plan": os.path.join(HAUPTORDNER, "clean_tk_plan.gpkg"),
    "clean_vf_1000": os.path.join(HAUPTORDNER, "clean_vf_1000.gpkg"),
    "analysis_merged": os.path.join(HAUPTORDNER, "04_analysis_merged.gpkg"),
    "master_analysis": os.path.join(HAUPTORDNER, "05_master_analysis.gpkg")
}

OUTPUT_MAP_PNG = os.path.join(HAUPTORDNER, "berlin_strategie_karte.png")
OUTPUT_MAP_HTML = os.path.join(HAUPTORDNER, "berlin_interaktiv.html")

# --- GEO & PROJEKTION ---
ANALYSIS_CRS = "EPSG:25833" # ETRS89 / UTM zone 33N (Berlin Standard)
WEB_CRS = "EPSG:4326"       # WGS84 (Folium/Web)

# Download Bounding Box (Berlin Umgebung)
DOWNLOAD_BBOX = {
   "X_START": 1450000.0,
   "Y_START": 6940000.0,
   "X_ENDE": 1540000.0,
   "Y_ENDE": 6840000.0
}

# --- EXTERNE QUELLEN (WFS) ---
URLS = {
    "bezirke": "https://gdi.berlin.de/services/wfs/alkis_bezirke?service=wfs&version=2.0.0&request=GetFeature&typeNames=alkis_bezirke:bezirksgrenzen&outputFormat=application/json&srsName=EPSG:25833",
    "flaechennutzung": "https://gdi.berlin.de/services/wfs/ua_flaechennutzung?service=wfs&version=2.0.0&request=GetFeature&typeNames=ua_flaechennutzung:c_reale_nutzung_2023&outputFormat=application/json&srsName=EPSG:25833"
}

# --- VISUALISIERUNG ---
COLORS = {
    "Wettbewerb": "#228B22",       # Forest Green
    "Telekom": "#E20074",          # Telekom Magenta
    "Vodafone": "#E60000",         # Vodafone Rot
    "Geplant": "#1E90FF",          # Dodger Blue
    "Lücke (White Spot)": "#FF8C00",# Dark Orange
    "Sonstiges": "#D3D3D3"         # Light Grey
}

# --- PIPELINE SCHRITTE ---
PIPELINE_STEPS = [
    ("1. Download Phase", "s01_downloader"),
    ("2. Processing Phase", "s02_processor"),
    ("3. Cleaning Phase", "s03_cleaning"),
    ("4. Analysis Phase", "s04_analysis"),
    ("5. Enrichment Phase", "s05_enrichment"),
    ("6. Visualization Phase", "s06_visualization")
]
