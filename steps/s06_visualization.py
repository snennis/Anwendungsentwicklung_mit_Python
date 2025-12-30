import os
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
import contextily as cx
import logging
from datetime import datetime
from config import get_log_path, VISUALIZATION_INPUT_GPKG, VISUALIZATION_MAP_PNG, VISUALIZATION_COLORS

INPUT_GPKG = VISUALIZATION_INPUT_GPKG
OUTPUT_MAP_PNG = VISUALIZATION_MAP_PNG
LOG_FILE = get_log_path("06_visualization.log")
COLORS = VISUALIZATION_COLORS

# Mapping: ARS-Schlüssel -> Name
DISTRICT_MAPPING = {
    '11000001': 'Mitte',
    '11000002': 'Friedrichshain-Kr.',
    '11000003': 'Pankow',
    '11000004': 'Charlottenburg-Wilm.',
    '11000005': 'Spandau',
    '11000006': 'Steglitz-Zehl.',
    '11000007': 'Tempelhof-Schön.',
    '11000008': 'Neukölln',
    '11000009': 'Treptow-Köpenick',
    '11000010': 'Marzahn-Hellersdorf',
    '11000011': 'Lichtenberg',
    '11000012': 'Reinickendorf'
}

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'), logging.StreamHandler()]
    )

def add_north_arrow(ax):
    """Fügt einen stilisierten Nordpfeil oben rechts hinzu."""
    x, y, arrow_length = 0.97, 0.95, 0.05
    ax.annotate('N', xy=(x, y), xytext=(x, y-arrow_length),
                arrowprops=dict(facecolor='black', width=4, headwidth=12),
                ha='center', va='center', fontsize=10,
                xycoords=ax.transAxes, zorder=10)

def main():
    setup_logging()
    logging.info("🚀 STARTE VISUALISIERUNG (Final Fix)")

    if not os.path.exists(INPUT_GPKG):
        logging.error(f"Input fehlt: {INPUT_GPKG}")
        return

    # 1. DATEN LADEN
    logging.info("Lade Geodaten...")
    try:
        gdf_blocks = gpd.read_file(INPUT_GPKG, layer="map_detail_nutzung", engine="pyogrio")
        gdf_bezirke = gpd.read_file(INPUT_GPKG, layer="map_stats_bezirke", engine="pyogrio")
        
        # WICHTIG: Reprojektion nach WebMercator (EPSG:3857) für Contextily Basemaps
        logging.info("   Reprojiziere nach WebMercator (EPSG:3857)...")
        if gdf_blocks.crs != "EPSG:3857":
            gdf_blocks = gdf_blocks.to_crs(epsg=3857)
        if gdf_bezirke.crs != "EPSG:3857":
            gdf_bezirke = gdf_bezirke.to_crs(epsg=3857)

        # 2. NAMEN STATT IDS (Robuste Suche)
        logging.info("   Löse Bezirksnamen auf...")
        # Suche Spalte, die wie eine ID aussieht (beginnt mit '11' und ist lang)
        id_col = None
        for col in gdf_bezirke.columns:
            # Check erstes Element
            val = str(gdf_bezirke[col].iloc[0])
            if val.startswith('11') and len(val) >= 8:
                id_col = col
                break
        
        gdf_bezirke['display_name'] = "Bezirk"
        if id_col:
            logging.info(f"   -> ID-Spalte gefunden: {id_col}")
            # Clean: Whitespace weg, String erzwingen
            gdf_bezirke['clean_id'] = gdf_bezirke[id_col].astype(str).str.strip()
            gdf_bezirke['display_name'] = gdf_bezirke['clean_id'].map(DISTRICT_MAPPING).fillna(gdf_bezirke['clean_id'])
        else:
            # Fallback auf Namensspalte
            name_col = next((c for c in gdf_bezirke.columns if c.lower() in ['name', 'bezeichnung', 'nam']), None)
            if name_col: gdf_bezirke['display_name'] = gdf_bezirke[name_col]

    except Exception as e:
        logging.error(f"Fehler beim Laden/Verarbeiten: {e}")
        return

    # ---------------------------------------------------------
    # PLOTTING
    # ---------------------------------------------------------
    logging.info("Erstelle Karte...")
    
    # 20x20 Zoll ist gut für Details
    fig, ax = plt.subplots(figsize=(20, 20)) 
    
    # GRENZEN SETZEN (Ganz wichtig gegen das "weiße Bild")
    # Wir nehmen die exakten Grenzen der Bezirke
    minx, miny, maxx, maxy = gdf_bezirke.total_bounds
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    # 1. BASEMAP (Hintergrundkarte)
    logging.info("   Lade Basemap (CartoDB Positron)...")
    try:
        # Source explizit angeben. crs=... ist wichtig!
        cx.add_basemap(
            ax, 
            crs=gdf_bezirke.crs.to_string(), 
            source=cx.providers.CartoDB.PositronNoLabels, # NoLabels, damit wir unsere eigenen Namen nutzen können
            attribution=False,
            zoom=12 # Fixer Zoom verhindert Speicherüberlauf (Memory Error)
        )
    except Exception as e:
        logging.warning(f"Konnte Basemap nicht laden: {e}")

    # 2. DATEN (Versorgungslücken etc.)
    # Farben vorbereiten
    gdf_blocks['color'] = gdf_blocks['versorgung_visual'].map(COLORS).fillna("#d3d3d3")

    logging.info("   Rendere Versorgungsdaten...")
    gdf_blocks.plot(
        ax=ax, 
        color=gdf_blocks['color'], 
        edgecolor='none', 
        alpha=0.65,  # Transparent, damit Straßen sichtbar bleiben
        zorder=2
    )

    # 3. BEZIRKSRAHMEN
    gdf_bezirke.plot(
        ax=ax,
        facecolor="none",
        edgecolor="#444444", 
        linewidth=1.5,
        zorder=3,
        alpha=0.8
    )

    # 4. LABELS (Namen)
    logging.info("   Plaziere Labels...")
    for idx, row in gdf_bezirke.iterrows():
        # Representative Point garantiert Position IM Polygon
        pt = row.geometry.representative_point()
        txt = ax.text(
            pt.x, pt.y, 
            str(row['display_name']).upper(), 
            ha='center', va='center', 
            fontsize=12, 
            fontweight='bold',
            color='#222222', 
            zorder=4
        )
        # Weißer Rand (Halo) für Lesbarkeit
        txt.set_path_effects([pe.withStroke(linewidth=4, foreground='white', alpha=0.8)])

    # 5. LEGENDE & DEKO
    patches = [mpatches.Patch(color=c, label=l) for l, c in COLORS.items()]
    leg = plt.legend(
        handles=patches, 
        loc='lower right', 
        title="Versorgungsstatus", 
        fontsize=12, 
        title_fontsize=14,
        frameon=True,
        facecolor='white',
        framealpha=0.9, 
        borderpad=1,
        shadow=True
    )

    add_north_arrow(ax)

    # Titelbox oben links
    props = dict(boxstyle='round', facecolor='white', alpha=0.9, pad=0.5)
    ax.text(0.02, 0.98, "Glasfaser-Analyse Berlin", transform=ax.transAxes, fontsize=24,
            verticalalignment='top', fontweight='bold', bbox=props, zorder=5)
    
    ax.text(0.02, 0.945, "Identifikation von 'White Spots' in Wohn- & Gewerbegebieten", 
            transform=ax.transAxes, fontsize=12, verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8), zorder=5)

    # Credits unten links
    date_str = datetime.now().strftime("%d.%m.%Y")
    ax.text(0.02, 0.01, 
             f"Basemap: © CartoDB, © OSM | Analyse: Python ETL | Stand: {date_str}", 
             transform=ax.transAxes, fontsize=9, color='#333333', zorder=5,
             path_effects=[pe.withStroke(linewidth=2, foreground='white')])

    # Achsen aus
    ax.set_axis_off()

    # Speichern
    logging.info("   Speichere PNG (200 DPI)...")
    # DPI 200 ist guter Kompromiss zwischen Qualität und Dateigröße
    plt.savefig(OUTPUT_MAP_PNG, dpi=200, bbox_inches='tight', pad_inches=0.1) 
    logging.info(f"✅ Karte gespeichert: {OUTPUT_MAP_PNG}")
    plt.close()

    logging.info("✅ Visualisierung abgeschlossen.")

if __name__ == "__main__":
    main()