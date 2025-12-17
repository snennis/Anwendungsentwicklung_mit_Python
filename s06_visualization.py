import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import folium
from folium.plugins import Fullscreen
import config
import utils

def main():
    logger = utils.setup_logger("VISUALIZATION", config.LOG_FILES["s06"])
    logger.info("🚀 STARTE VISUALISIERUNG")

    input_gpkg = config.GPKG_FILES["master_analysis"]

    if not os.path.exists(config.HAUPTORDNER):
        logger.error(f"Hauptordner fehlt: {config.HAUPTORDNER}")
        return

    if not os.path.exists(input_gpkg):
        logger.error(f"Input fehlt: {input_gpkg}")
        return

    # 1. DATEN LADEN
    logger.info("Lade Geodaten...")
    try:
        # Layer 1: Die detaillierten Blöcke
        gdf_blocks = gpd.read_file(input_gpkg, layer="map_detail_nutzung")
        # Layer 2: Die Bezirke (für Rahmen)
        gdf_bezirke = gpd.read_file(input_gpkg, layer="map_stats_bezirke")
    except Exception as e:
        logger.error(f"Fehler beim Laden: {e}")
        return

    # ---------------------------------------------------------
    # TEIL 1: STATISCHE KARTE (Matplotlib)
    # ---------------------------------------------------------
    logger.info("Erstelle statische Strategie-Karte (PNG)...")
    
    # Setup Plot
    fig, ax = plt.subplots(figsize=(20, 15)) # Großes Format
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#f0f0f0') # Leichter Hintergrundgrau

    # Daten filtern: Wir zeigen ALLES, aber färben unterschiedlich
    # Mapping der Farben basierend auf der Spalte 'versorgung_visual'
    # Falls Werte im GDF sind, die nicht im Dict stehen, nehmen wir Grau
    gdf_blocks['color'] = gdf_blocks['versorgung_visual'].map(config.COLORS).fillna("#808080")

    # Plotten
    logger.info("   Rendere Polygone...")
    gdf_blocks.plot(
        ax=ax, 
        color=gdf_blocks['color'], 
        edgecolor='none', # Keine Ränder für Performance und Look
        alpha=0.8
    )

    # Bezirksgrenzen darüber legen
    logger.info("   Zeichne Bezirksgrenzen...")
    gdf_bezirke.plot(
        ax=ax,
        facecolor="none",
        edgecolor="black",
        linewidth=1.5,
        alpha=0.5
    )

    # Legende bauen
    patches = [mpatches.Patch(color=c, label=l) for l, c in config.COLORS.items()]
    plt.legend(handles=patches, loc='lower right', title="Versorgungsstatus", fontsize=12, title_fontsize=14)

    # Texte & Titel
    plt.title("Glasfaser-Versorgungsanalyse Berlin (Wohnen & Gewerbe)", fontsize=24, fontweight='bold', pad=20)
    plt.suptitle("Datenquellen: Telekom, Vodafone, GDI Berlin (Open Data) | Analyse: Python ETL Pipeline", fontsize=12, y=0.92)
    
    # Achsen ausblenden
    ax.set_axis_off()

    # Speichern
    plt.savefig(config.OUTPUT_MAP_PNG, dpi=150, bbox_inches='tight') # DPI 150 reicht für Monitor, 300 für Druck
    logger.info(f"✅ PNG gespeichert: {config.OUTPUT_MAP_PNG}")
    plt.close()

    # ---------------------------------------------------------
    # TEIL 2: INTERAKTIVE KARTE (Folium)
    # ---------------------------------------------------------
    logger.info("Erstelle interaktive Web-Karte (HTML)...")

    # GeoPandas muss für Folium immer EPSG:4326 (Lat/Lon) sein
    if gdf_blocks.crs != config.WEB_CRS:
        gdf_blocks_web = gdf_blocks.to_crs(config.WEB_CRS)
    else:
        gdf_blocks_web = gdf_blocks
        
    if gdf_bezirke.crs != config.WEB_CRS:
        gdf_bezirke_web = gdf_bezirke.to_crs(config.WEB_CRS)
    else:
        gdf_bezirke_web = gdf_bezirke

    # Karte initialisieren (Zentrum Berlin)
    m = folium.Map(location=[52.5200, 13.4050], zoom_start=11, tiles="CartoDB positron")
    Fullscreen().add_to(m)

    # Funktion für Styling (Farbe je nach Status)
    def style_function(feature):
        status = feature['properties']['versorgung_visual']
        return {
            'fillColor': config.COLORS.get(status, '#808080'),
            'color': 'none', # Keine Umrandung
            'weight': 0,
            'fillOpacity': 0.7
        }

    # Layer-Definitionen (Name im GDF, Anzeigename, Standardmäßig sichtbar?)
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
    # Funktion für Farbe je nach Prozentwert (0-100)
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

    # 3b. Layer: Bezirke (Nur Rahmen) - Immer verfügbar für Orientierung
    folium.GeoJson(
        gdf_bezirke_web,
        name="Bezirksgrenzen (Rahmen)",
        style_function=lambda x: {'color': 'black', 'fillColor': 'transparent', 'weight': 2, 'pointer_events': False},
        highlight_function=lambda x: {'weight': 4, 'color': '#666'},
        show=True
    ).add_to(m)

    folium.LayerControl().add_to(m)

    m.save(config.OUTPUT_MAP_HTML)
    logger.info(f"✅ HTML gespeichert: {config.OUTPUT_MAP_HTML}")

if __name__ == "__main__":
    main()