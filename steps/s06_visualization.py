import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import folium
from folium.plugins import Fullscreen
import logging
from config import BASE_DIR, get_log_path, VISUALIZATION_INPUT_GPKG, VISUALIZATION_MAP_PNG, VISUALIZATION_MAP_HTML, VISUALIZATION_COLORS

INPUT_GPKG = VISUALIZATION_INPUT_GPKG
OUTPUT_MAP_PNG = VISUALIZATION_MAP_PNG
OUTPUT_MAP_HTML = VISUALIZATION_MAP_HTML
LOG_FILE = get_log_path("06_visualization.log")
COLORS = VISUALIZATION_COLORS

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'), logging.StreamHandler()]
    )

def main():
    setup_logging()
    logging.info("🚀 STARTE VISUALISIERUNG")

    if not os.path.exists(INPUT_GPKG):
        logging.error(f"Input fehlt: {INPUT_GPKG}")
        return

    # 1. DATEN LADEN
    logging.info("Lade Geodaten...")
    try:
        # Layer 1: Die detaillierten Blöcke
        gdf_blocks = gpd.read_file(INPUT_GPKG, layer="map_detail_nutzung", engine="pyogrio")
        # Layer 2: Die Bezirke (für Rahmen)
        gdf_bezirke = gpd.read_file(INPUT_GPKG, layer="map_stats_bezirke", engine="pyogrio")
    except Exception as e:
        logging.error(f"Fehler beim Laden: {e}")
        return

    # ---------------------------------------------------------
    # TEIL 1: STATISCHE KARTE (Matplotlib)
    # ---------------------------------------------------------
    logging.info("Erstelle statische Strategie-Karte (PNG)...")
    
    # Setup Plot
    fig, ax = plt.subplots(figsize=(20, 15)) # Großes Format
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#f0f0f0') # Leichter Hintergrundgrau

    # Daten filtern: Wir zeigen ALLES, aber färben unterschiedlich
    gdf_blocks['color'] = gdf_blocks['versorgung_visual'].map(COLORS).fillna("#808080")

    # Plotten
    logging.info("   Rendere Polygone...")
    gdf_blocks.plot(
        ax=ax, 
        color=gdf_blocks['color'], 
        edgecolor='none', 
        alpha=0.8
    )

    # Bezirksgrenzen darüber legen
    logging.info("   Zeichne Bezirksgrenzen...")
    gdf_bezirke.plot(
        ax=ax,
        facecolor="none",
        edgecolor="black",
        linewidth=1.5,
        alpha=0.5
    )

    # Legende bauen
    patches = [mpatches.Patch(color=c, label=l) for l, c in COLORS.items()]
    plt.legend(handles=patches, loc='lower right', title="Versorgungsstatus", fontsize=12, title_fontsize=14)

    # Texte & Titel
    plt.title("Glasfaser-Versorgungsanalyse Berlin (Wohnen & Gewerbe)", fontsize=24, fontweight='bold', pad=20)
    plt.suptitle("Datenquellen: Telekom, Vodafone, GDI Berlin (Open Data) | Analyse: Python ETL Pipeline", fontsize=12, y=0.92)
    
    # Achsen ausblenden
    ax.set_axis_off()

    # Speichern
    plt.savefig(OUTPUT_MAP_PNG, dpi=150, bbox_inches='tight') 
    logging.info(f"✅ PNG gespeichert: {OUTPUT_MAP_PNG}")
    plt.close()

    # ---------------------------------------------------------
    # TEIL 2: INTERAKTIVE KARTE (Folium)
    # ---------------------------------------------------------
    logging.info("Erstelle interaktive Web-Karte (HTML)...")

    # GeoPandas muss für Folium immer EPSG:4326 (Lat/Lon) sein
    if gdf_blocks.crs != "EPSG:4326":
        gdf_blocks_web = gdf_blocks.to_crs("EPSG:4326")
    else:
        gdf_blocks_web = gdf_blocks
        
    if gdf_bezirke.crs != "EPSG:4326":
        gdf_bezirke_web = gdf_bezirke.to_crs("EPSG:4326")
    else:
        gdf_bezirke_web = gdf_bezirke

    # Karte initialisieren (Zentrum Berlin)
    m = folium.Map(location=[52.5200, 13.4050], zoom_start=11, tiles="CartoDB positron")
    Fullscreen().add_to(m)

    # Funktion für Styling (Farbe je nach Status)
    def style_function(feature):
        status = feature['properties']['versorgung_visual']
        return {
            'fillColor': COLORS.get(status, '#808080'),
            'color': 'none', # Keine Umrandung
            'weight': 0,
            'fillOpacity': 0.7
        }

    # Layer-Definitionen
    layers_config = [
        ("Lücke (White Spot)", "🔴 White Spots (Lücken)", True),
        ("Geplant", "🔵 Ausbau Geplant", True),
        ("Wettbewerb", "🟢 Wettbewerb", False),
        ("Telekom", "🟣 Telekom", False),
        ("Vodafone", "🔴 Vodafone", False),
        ("Sonstiges", "⚪ Sonstiges", False)
    ]

    for cat_key, cat_label, show_default in layers_config:
        subset = gdf_blocks_web[gdf_blocks_web['versorgung_visual'] == cat_key]
        if not subset.empty:
            folium.GeoJson(
                subset,
                name=cat_label,
                style_function=style_function,
                tooltip=folium.GeoJsonTooltip(fields=['kategorie', 'versorgung_visual'], aliases=['Nutzung:', 'Status:']),
                show=show_default
            ).add_to(m)

    # 3a. Layer: Bezirke (Choropleth - Versorgung)
    def get_district_color(pct):
        if pct < 50: return '#d7191c' # Rot
        elif pct < 70: return '#fdae61' # Orange
        elif pct < 90: return '#a6d96a' # Hellgrün
        else: return '#1a9641' # Dunkelgrün

    folium.GeoJson(
        gdf_bezirke_web,
        name="Bezirke (Versorgung %)",
        style_function=lambda x: {
            'fillColor': get_district_color(x['properties'].get('Wohn_Versorgt_Pct', 0)),
            'color': 'gray',
            'weight': 1,
            'fillOpacity': 0.4
        },
        tooltip=folium.GeoJsonTooltip(fields=['Bezirk', 'Wohn_Versorgt_Pct'], aliases=['Bezirk:', 'Versorgt (%):']),
        show=False
    ).add_to(m)

    # 3b. Layer: Bezirke (Nur Rahmen)
    folium.GeoJson(
        gdf_bezirke_web,
        name="Bezirksgrenzen (Rahmen)",
        style_function=lambda x: {'color': 'black', 'fillColor': 'transparent', 'weight': 2, 'pointer_events': False},
        highlight_function=lambda x: {'weight': 4, 'color': '#666'},
        show=True
    ).add_to(m)

    folium.LayerControl().add_to(m)

    m.save(OUTPUT_MAP_HTML)
    logging.info(f"✅ HTML gespeichert: {OUTPUT_MAP_HTML}")

if __name__ == "__main__":
    main()
