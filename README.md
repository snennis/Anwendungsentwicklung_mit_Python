# **📡 Fiber Optic Intelligence Platform (Berlin/Brandenburg)**

Eine automatisierte **ETL-Pipeline** zur Analyse der Glasfaser-Versorgungssituation (FTTH) in Berlin und Brandenburg. Das System extrahiert Daten aus verschiedenen Provider-Schnittstellen (WMS & ArcGIS REST), transformiert Rasterdaten in saubere Vektorgeometrien und führt komplexe räumliche Analysen durch, um Marktsituationen (Monopole vs. Wettbewerb) und Versorgungslücken ("White Spots") zu identifizieren.

## **🚀 Features**

* **High-Performance Ingestion:** Multi-threaded Downloader ("Scatter-Gather" Pattern) für Telekom- und Vodafone-Netzkarten.  
* **Raster-to-Vector Engine:** Speichereffiziente Stream-Verarbeitung zur Umwandlung von Pixeldaten in Vektor-Polygone.  
* **Advanced Geometry Cleaning:** Automatische Reparatur von Topologie-Fehlern:  
  * Schließen von Artefakten (Kachel-Ränder).  
  * Auffüllen von Mustern (z.B. 11,5m Rasterlöcher der Telekom) mittels Morphological Buffering.  
* **Spatial Intelligence:** Berechnung von:  
  * Wettbewerbszonen (Overlay-Analyse).  
  * Provider-Monopolen.  
  * Strategischen Überbauungen (Planung vs. Bestand).  
  * Versorgungslücken im Vergleich zur Landesfläche.

## **🏗️ Architektur & Pipeline**

Das Projekt folgt einer strikten "Separation of Concerns" Architektur in 4 Phasen, orchestriert durch pipeline\_manager.py:

graph LR  
    A\[01 Downloader\] \--\>|Raw Tiles| B\[02 Processor\]  
    B \--\>|Raw Vectors| C\[03 Cleaner\]  
    C \--\>|Clean Vectors| D\[04 Analyzer\]  
    D \--\>|Insights| E\[GeoPackage / Stats\]

### **1\. Download Phase (s01\_downloader.py)**

Nutzt ThreadPoolExecutor für parallele Requests. Unterstützt WMS (Telekom) und ArcGIS REST (Vodafone) Protokolle. Intelligentes Caching verhindert redundante Downloads.

### **2\. Processing Phase (s02\_processor.py)**

Vektorisierung der Rasterdaten mittels rasterio und shapely. Enthält einen **Memory-Safe Iterator**, der auch riesige Datensätze ohne RAM-Overflow verarbeitet. Nutzt scipy für morphologisches Schließen kleiner Pixel-Lücken.

### **3\. Cleaning Phase (s03\_cleaning.py)**

Geometrische Reparatur der Rohdaten. Wendet einen **Buffer-Dissolve-Unbuffer** Algorithmus an, um "Korridore" und systematische Lücken in den Provider-Daten zu schließen und saubere Flächen für die Flächenberechnung zu erzeugen.

### **4\. Analysis Phase (s04\_analysis.py)**

Führt die Mengenlehre (Intersection, Difference, Union) auf den bereinigten Layern durch. Projiziert Daten nach **EPSG:25833 (ETRS89 / UTM zone 33N)** für präzise Flächenberechnungen in km².

## **🛠️ Installation**

### **Voraussetzungen**

* Python 3.9 oder höher  
* Empfohlen: Ein virtuelles Environment (venv oder conda)

### **Setup**

1. **Repository klonen**  
   git clone \[https://github.com/snennis/Anwendungsentwicklung\_mit\_Python.git\](https://github.com/snennis/Anwendungsentwicklung\_mit\_Python.git)  
   cd Anwendungsentwicklung\_mit\_Python

2. Abhängigkeiten installieren  
   Die Analyse benötigt diverse GIS-Bibliotheken (GDAL, Rasterio, GeoPandas).  
   pip install \-r requirements.txt

   *Hinweis für Windows-Nutzer: Falls die Installation von fiona oder rasterio fehlschlägt, nutzen Sie bitte vorkompilierte Wheels oder conda install geopandas.*

## **💻 Nutzung**

Die gesamte Pipeline wird über den zentralen Manager gesteuert. Dieser kümmert sich um Logging, Zeitmessung und Speicherbereinigung.

python pipeline\_manager.py

### **Output**

Die Ergebnisse landen im Ordner Glasfaser\_Analyse\_Project:

* pipeline\_run.log: Detaillierte Logs aller Schritte.  
* 04\_analysis\_merged.gpkg: Das finale GeoPackage mit allen Layern (Wettbewerb, Monopole, White Spots) und Attributen.

## **📊 Beispiel-Statistik (Auszug)**

Das System generiert am Ende einen Report über die Flächennutzung:

\==============================  
📊 STATISTIK (Merged Layer)  
\==============================  
                               Area (km²)  
status  
Monopol Telekom                452.30  
Monopol Vodafone               120.15  
Wettbewerb                      85.40  
White Spot                    1250.00  
\==============================

## **⚠️ Disclaimer**

Dieses Projekt dient ausschließlich wissenschaftlichen und bildenden Zwecken im Rahmen eines Geoinformatik-Studiums. Die Daten werden von öffentlichen Karten-Schnittstellen der Provider bezogen. Bitte beachten Sie die Nutzungsbedingungen der jeweiligen Diensteanbieter.