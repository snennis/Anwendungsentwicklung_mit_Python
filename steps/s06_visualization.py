import os
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import matplotlib.patheffects as pe
import contextily as cx
import logging
from datetime import datetime
from config import get_log_path, VISUALIZATION_INPUT_GPKG, VISUALIZATION_MAP_PNG, VISUALIZATION_COLORS, DISTRICT_MAPPING

INPUT_GPKG = VISUALIZATION_INPUT_GPKG
OUTPUT_MAP_PNG = VISUALIZATION_MAP_PNG
COLORS = VISUALIZATION_COLORS

# Hilfsfunktion für Legenden-Überschriften
def create_legend_header(label):
    # Ein unsichtbarer Patch als Platzhalter für Überschriften
    return mpatches.Patch(color='none', label=label)

def main():
    logging.info("🚀 STARTE VISUALISIERUNG (Final Strategic Layout)")

    if not os.path.exists(INPUT_GPKG):
        logging.error(f"Input fehlt: {INPUT_GPKG}")
        return

    # 1. DATEN LADEN
    logging.info("Lade Geodaten...")
    try:
        gdf_blocks = gpd.read_file(INPUT_GPKG, layer="map_detail_nutzung", engine="pyogrio")
        gdf_bezirke = gpd.read_file(INPUT_GPKG, layer="map_stats_bezirke", engine="pyogrio")
        
        if gdf_blocks.crs != "EPSG:3857": gdf_blocks = gdf_blocks.to_crs(epsg=3857)
        if gdf_bezirke.crs != "EPSG:3857": gdf_bezirke = gdf_bezirke.to_crs(epsg=3857)

        # Namen mappen (wie gehabt)
        id_col = next((c for c in gdf_bezirke.columns if str(gdf_bezirke[c].iloc[0]).startswith('11')), None)
        name_col = next((c for c in gdf_bezirke.columns if c.lower() in ['name', 'bezeichnung', 'nam']), None)
        gdf_bezirke['label'] = "Bezirk"
        if id_col:
            gdf_bezirke['clean_id'] = gdf_bezirke[id_col].astype(str).str.strip()
            gdf_bezirke['label'] = gdf_bezirke['clean_id'].map(DISTRICT_MAPPING).fillna(gdf_bezirke['clean_id'])
        elif name_col:
            gdf_bezirke['label'] = gdf_bezirke[name_col]

    except Exception as e:
        logging.error(f"Fehler beim Laden: {e}")
        return

    # ---------------------------------------------------------
    # PLOTTING (Strategisches Layout)
    # ---------------------------------------------------------
    logging.info("Erstelle Karte...")
    
    # Sehr großes Format für viel Weißraum und Details
    fig, ax = plt.subplots(figsize=(28, 24))
    
    # Zoom auf Berlin mit VIEL PADDING -> Macht Berlin kleiner
    minx, miny, maxx, maxy = gdf_bezirke.total_bounds
    pad_x = 4500 # Viel Platz links/rechts
    pad_y = 3500 # Viel Platz oben/unten
    ax.set_xlim(minx - pad_x, maxx + pad_x)
    ax.set_ylim(miny - pad_y, maxy + pad_y)

    # 2. BASEMAP
    logging.info("Lade Basemap...")
    try:
        # Positron, dezent
        cx.add_basemap(ax, crs=gdf_bezirke.crs.to_string(), source=cx.providers.CartoDB.PositronNoLabels, zoom=11, alpha=0.8)
    except: pass

    # 3. DATEN RENDEREN
    logging.info("Rendere Polygone...")
    gdf_blocks['color'] = gdf_blocks['versorgung_visual'].map(COLORS).fillna("#d3d3d3")
    gdf_blocks.plot(
        ax=ax, color=gdf_blocks['color'], edgecolor='none', alpha=0.85, zorder=2
    )

    # 4. GRENZEN & LABELS (Viel fetter und größer!)
    logging.info("Zeichne Grenzen und Labels...")
    # Fette, schwarze Grenzen
    gdf_bezirke.plot(
        ax=ax, facecolor="none", edgecolor="black", linewidth=2.5, zorder=3, alpha=0.7
    )

    for _, row in gdf_bezirke.iterrows():
        pt = row.geometry.representative_point()
        # Große, fette Labels mit starkem Halo
        txt = ax.text(
            pt.x, pt.y, str(row['label']).upper(),
            ha='center', va='center', 
            fontsize=16, # DEUTLICH GRÖSSER
            fontweight='bold', color='black', zorder=4
        )
        # Fetter weißer Rand für Lesbarkeit
        txt.set_path_effects([pe.withStroke(linewidth=4, foreground='white', alpha=0.8)])

    # 5. KOMBINIERTE LEGENDE
    logging.info("Erstelle kombinierte Legende...")
    legend_handles = []

    # Abschnitt 1: Bestand
    legend_handles.append(create_legend_header(r"$\bf{NETZ-STATUS}$")) # Fett via Mathtext
    status_keys = ["Wettbewerb", "Telekom", "Vodafone", "Geplant"]
    for k in status_keys:
        if k in COLORS: legend_handles.append(mpatches.Patch(color=COLORS[k], label=k))

    # Abstandhalter
    legend_handles.append(mpatches.Patch(color='none', label=' '))

    # Abschnitt 2: Potenzial
    legend_handles.append(create_legend_header(r"$\bf{VERTRIEBS-POTENZIAL}$"))
    legend_handles.append(create_legend_header("(Unversorgte Gebiete)"))
    pot_keys = ["Potenzial (Hoch)", "Potenzial (Mittel)", "Potenzial (Niedrig)"]
    for k in pot_keys:
        if k in COLORS: legend_handles.append(mpatches.Patch(color=COLORS[k], label=k))

    # Legende platzieren (Oben Rechts)
    leg = ax.legend(
        handles=legend_handles, loc='upper right',
        fontsize=12, frameon=True, facecolor='white', framealpha=0.95, edgecolor='black',
        bbox_to_anchor=(0.99, 0.99), borderpad=1.2, labelspacing=0.6
    )
    # Überschriften in der Legende linksbündig machen (Trick)
    for text in leg.get_texts():
        if text.get_text().startswith(r"$\bf") or text.get_text().startswith("("):
            text.set_ha("left")
            text.set_position((-15, 0)) # Nach links schieben

    # 6. HEADER-BLOCK (Titel + Erklärung)
    logging.info("Erstelle Header & Footer...")

    # Haupttitel (Riesig, Fett)
    ax.text(0.01, 0.99, "STRATEGISCHE GLASFASER-ANALYSE BERLIN", transform=ax.transAxes,
            fontsize=30, fontweight='heavy', color='black', va='top', ha='left', zorder=5)
    
    # Erklärender Textblock direkt darunter
    explanation_text = (
        "Diese Karte visualisiert die Versorgungssituation mit gigabitfähiger Infrastruktur (FTTH/Coax).\n"
        "Im Fokus steht die Identifikation wirtschaftlich relevanter 'White Spots' (unversorgte Gebiete).\n"
        "Dafür wurden Netzdaten geometrisch mit der realen Flächennutzung (Wohnen/Gewerbe vs. Natur) verschnitten.\n"
        "Das Ergebnis priorisiert Lücken nach ihrem Vertriebspotenzial."
    )

    # Textbox für bessere Lesbarkeit auf Basemap
    props = dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='none', pad=0.8)
    ax.text(0.01, 0.945, explanation_text, transform=ax.transAxes,
            fontsize=15, color='#333333', va='top', ha='left', bbox=props, zorder=5, linespacing=1.5)


    # 7. QUELLEN-BLOCK (Unten Links, aufgeräumt)
    today = datetime.now().strftime("%d.%m.%Y")
    # Nutzung von Fettung via Mathtext für Keys
    source_text = (
        r"$\bf{DATENBASIS:}$" + "\nProvider-Daten (WMS/REST Schnittstellen) & ALKIS/ISU5 (GDI Berlin).\n\n" +
        r"$\bf{METHODIK:}$" + "\nGeometrische Differenzanalyse mit nachgelagerter Nutzungsklassifizierung.\n" +
        "High Potential = Wohn-/Mischgebiete | Low Potential = Wald/Wasser/Verkehr.\n\n" +
        r"$\bf{STAND:}$ " + f"{today} | Erstellt mit Python ETL Pipeline"
    )

    # Platzierung unten links
    ax.text(0.01, 0.01, source_text, transform=ax.transAxes,
            fontsize=12, color='black', va='bottom', ha='left', bbox=props, zorder=5, linespacing=1.4)

    # Achsen aus, Nordpfeil weg.
    ax.set_axis_off()

    logging.info("Speichere PNG (High-Res)...")
    # Hohe DPI für gestochen scharfe Texte
    plt.savefig(OUTPUT_MAP_PNG, dpi=250, bbox_inches='tight', pad_inches=0.3)
    logging.info(f"✅ Fertig: {OUTPUT_MAP_PNG}")
    plt.close()

if __name__ == "__main__":
    main()