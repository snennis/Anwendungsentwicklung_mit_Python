# **📡 Fiber Optic Intelligence Platform**

Eine spezialisierte **Spatial ETL-Pipeline** zur Analyse der Glasfaser-Versorgungssituation (FTTH) in Berlin. Das System extrahiert Rasterdaten von Provider-Schnittstellen (WMS & ArcGIS REST), transformiert diese in bereinigte Vektorgeometrien und verschneidet sie mit administrativen Daten (WFS), um "White Spots" und Monopolstellungen präzise zu lokalisieren.

## **🚀 Key Features**

| Modul | Beschreibung |
| :---- | :---- |
| **⚡ Smart Ingestion** | Paralleler Downloader ("Scatter-Gather" Pattern) für Telekom- (WMS) und Vodafone-Netzkarten (ArcGIS REST) mit Caching-Strategie. |
| **🗺️ Vectorization Engine** | Effiziente Transformation von Raster-Kacheln in Vektor-Polygone (rasterio & shapely) unter Nutzung von Multiprocessing. |
| **🧹 Topology Cleaning** | Automatisierte Geometrie-Reparatur (Buffer-Dissolve-Unbuffer), um Artefakte zu entfernen und saubere Flächen für die Statistik zu gewährleisten. |
| **🧠 Spatial Analytics** | Mengenlehre-Operationen (Intersection, Difference) zur Ermittlung von Monopolen, Wettbewerbszonen und unversorgten Gebieten. |
| **🏙️ Context Enrichment** | Anreicherung der Daten durch WFS-Dienste (ALKIS Bezirke, Flächennutzung ISU5), um Lücken in Wohn- und Gewerbegebieten zu unterscheiden. |

## **🏗️ Architektur**

Das Projekt implementiert eine modulare Pipeline-Architektur mit strikter Trennung der Verantwortlichkeiten (SoC):

graph LR  
    A\[01 Ingestion\] \--\>|Raw Tiles| B\[02 Processing\]  
    B \--\>|Raw Vectors| C\[03 Cleaning\]  
    C \--\>|Clean Vectors| D\[04 Analysis\]  
    D \--\>|Stats| E\[05 Enrichment\]  
    E \--\>|Context| F\[06 Visualization\]  
    F \--\>|PNG/GPKG| G\[Output\]  
    style A fill:\#e1f5fe,stroke:\#01579b,stroke-width:2px  
    style B fill:\#e1f5fe,stroke:\#01579b,stroke-width:2px  
    style C fill:\#e8f5e9,stroke:\#2e7d32,stroke-width:2px  
    style D fill:\#e8f5e9,stroke:\#2e7d32,stroke-width:2px  
    style E fill:\#fff3e0,stroke:\#ef6c00,stroke-width:2px  
    style F fill:\#fff3e0,stroke:\#ef6c00,stroke-width:2px

### **Die Pipeline-Schritte**

1. **Downloader (s01):** Erntet Kacheln basierend auf einer Bounding Box. Umgeht Server-Limits durch intelligentes Threading.  
2. **Processor (s02):** Extrahiert Farbbereiche (z.B. Magenta für Telekom) aus den Bildern und konvertiert sie in Geometrien. Nutzt pyogrio für performantes Schreiben von GeoPackages.  
3. **Cleaning (s03):** Bereinigt die Roh-Vektoren. Schließt Lücken zwischen Kacheln und entfernt Rauschen durch morphologische Operationen.  
4. **Analysis (s04):** Berechnet die Marktanteile. Reprojiziert alles nach **EPSG:25833 (UTM 33N)** für exakte Flächenberechnungen.  
5. **Enrichment (s05):** Verbindet die Netzdaten mit der Flächennutzung.  
   * *Fragestellung:* "Welche Gewerbegebiete haben kein Glasfaser?"  
   * *Technik:* Spatial Join und Overlay-Analysen mit WFS-Live-Daten.  
6. **Visualization (s06):** Erstellt eine hochauflösende Strategie-Karte mittels matplotlib und contextily (Basemaps) sowie detaillierte Statistiken pro Bezirk.

## **📂 Projektstruktur**

fiber\_data/  
│   ├── cache/             \# Temporäre Speicher (Tiles, Roh-GPKGs)  
│   ├── logs/              \# Ausführliche Logs pro Schritt  
│   └── output/            \# Ergebnisse (Master-GPKG, Karten)  
├── config.py              \# Zentrale Konfiguration (URLs, Farben, Pfade)  
├── main.py                \# Pipeline-Manager & Entry Point  
├── steps/                 \# Modulare Logik  
│   ├── s01\_downloader.py  
│   ├── s02\_processor.py  
│   ├── s03\_cleaning.py  
│   ├── s04\_analysis.py  
│   ├── s05\_enrichment.py  
│   └── s06\_visualization.py  
└── requirements.txt

## **🛠️ Installation & Setup**

### **Voraussetzungen**

* **Python 3.9+**  
* Systembibliotheken für Geodaten (GDAL, PROJ)

### **Installation**

1. **Repository klonen**  
   git clone https://github.com/snennis/Anwendungsentwicklung_mit_Python.git  
   cd Anwendungsentwicklung\_mit\_Python

2. **Environment aufsetzen** Es wird empfohlen, conda zu nutzen, um Konflikte mit C-Bibliotheken (GDAL) zu vermeiden:  
   conda create \-n fiber\_intelligence python=3.10  
   conda activate fiber\_intelligence  
   pip install \-r requirements.txt  
   **Hinweis:** Das Projekt nutzt pyogrio als Engine für GeoPandas, um Schreibvorgänge drastisch zu beschleunigen. Stellen Sie sicher, dass dies korrekt installiert ist.

## **💻 Nutzung**

Die Pipeline ist vollautomatisiert. Der Manager (main.py) steuert den Ablauf, fängt Fehler ab und misst die Laufzeiten.

python main.py

### **Konfiguration**

Anpassungen an Untersuchungsgebiet (BBox), Provider-URLs oder Farbcodes können zentral in der config.py vorgenommen werden.

### **Output (fiber\_data/output/)**

* **05\_master\_analysis.gpkg**: Das vollständige GeoPackage. Enthält Layer für Monopole, Wettbewerb, Lücken und angereicherte Nutzungsdaten.  
* **berlin\_strategie\_karte.png**: Eine statische, druckfertige Karte der Versorgungssituation.  
* **Terminal-Report**: Eine Zusammenfassung der Flächenanteile (km²) direkt nach Durchlauf.

## **📊 Exemplarische Ergebnisse**

Das System liefert quantitative Aussagen zur digitalen Infrastruktur:

| Status | Fläche (km²) | Beschreibung |
| :---- | :---- | :---- |
| **Wettbewerb** | 74.63 | Infrastruktur beider Provider vorhanden |
| **Monopol Telekom** | 33.71 | Exklusive Versorgung durch Telekom |
| **Monopol Vodafone** | 246.16 | Exklusive Versorgung durch Vodafone (Coax/Fiber) |
| **White Spot** | 531.65 | Keine gigabitfähige Infrastruktur erkannt |

## **⚠️ Disclaimer**

Dieses Projekt ist eine akademische Arbeit im Rahmen des Studiengangs Geoinformatik (B. Eng.). Die verwendeten Daten stammen aus öffentlichen Quellen (WMS/REST/WFS). Die Analyse stellt eine Momentaufnahme dar und dient ausschließlich Bildungszwecken.