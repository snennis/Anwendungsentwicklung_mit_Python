# 📡 Fiber Optic Intelligence Platform (Berlin/Brandenburg)

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Educational-orange?style=for-the-badge)

Eine automatisierte **ETL-Pipeline** zur Analyse der Glasfaser-Versorgungssituation (FTTH) in Berlin und Brandenburg. Das System extrahiert Daten aus verschiedenen Provider-Schnittstellen (WMS & ArcGIS REST), transformiert Rasterdaten in saubere Vektorgeometrien und führt komplexe räumliche Analysen durch, um Marktsituationen (Monopole vs. Wettbewerb) und Versorgungslücken ("White Spots") zu identifizieren.

---

## 🚀 Features

| Feature | Beschreibung |
| :--- | :--- |
| **⚡ High-Performance Ingestion** | Multi-threaded Downloader ("Scatter-Gather" Pattern) für Telekom- und Vodafone-Netzkarten. |
| **🗺️ Raster-to-Vector Engine** | Speichereffiziente Stream-Verarbeitung zur Umwandlung von Pixeldaten in Vektor-Polygone. |
| **🧹 Advanced Geometry Cleaning** | Automatische Reparatur von Topologie-Fehlern (Schließen von Artefakten, Morphological Buffering). |
| **🧠 Spatial Intelligence** | Berechnung von Wettbewerbszonen, Monopolen, strategischen Überbauungen und Versorgungslücken. |
| **📊 Modular & Configurable** | Zentrale Konfiguration und modulare Architektur für einfache Erweiterbarkeit. |

---

## 🏗️ Architektur & Pipeline

Das Projekt folgt einer strikten "Separation of Concerns" Architektur in 6 Phasen, orchestriert durch `main.py`:

```mermaid
graph LR
    A[01 Downloader] -->|Raw Tiles| B[02 Processor]
    B -->|Raw Vectors| C[03 Cleaner]
    C -->|Clean Vectors| D[04 Analyzer]
    D -->|Base Analysis| E[05 Enrichment]
    E -->|Insights| F[06 Visualization]
    F -->|Maps| G[GeoPackage / PNG / HTML]
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style C fill:#bfb,stroke:#333,stroke-width:2px
    style D fill:#fbf,stroke:#333,stroke-width:2px
    style E fill:#fc9,stroke:#333,stroke-width:2px
    style F fill:#ff9,stroke:#333,stroke-width:2px
    style G fill:#ff9,stroke:#333,stroke-width:2px
```

### 1. Download Phase (`steps/s01_downloader.py`)
Nutzt `ThreadPoolExecutor` für parallele Requests. Unterstützt WMS (Telekom) und ArcGIS REST (Vodafone) Protokolle. Intelligentes Caching verhindert redundante Downloads.

### 2. Processing Phase (`steps/s02_processor.py`)
Vektorisierung der Rasterdaten mittels `rasterio` und `shapely`. Enthält einen **Memory-Safe Iterator**, der auch riesige Datensätze ohne RAM-Overflow verarbeitet. Nutzt `scipy` für morphologisches Schließen kleiner Pixel-Lücken.

### 3. Cleaning Phase (`steps/s03_cleaning.py`)
Geometrische Reparatur der Rohdaten. Wendet einen **Buffer-Dissolve-Unbuffer** Algorithmus an, um "Korridore" und systematische Lücken in den Provider-Daten zu schließen und saubere Flächen für die Flächenberechnung zu erzeugen.

### 4. Analysis Phase (`steps/s04_analysis.py`)
Führt die Mengenlehre (Intersection, Difference, Union) auf den bereinigten Layern durch. Projiziert Daten nach **EPSG:25833 (ETRS89 / UTM zone 33N)** für präzise Flächenberechnungen in km².

### 5. Enrichment Phase (`steps/s05_enrichment.py`)
Verknüpft die Analyse-Ergebnisse mit Kontextdaten (WFS Berlin):
*   **B2B-Potential**: Identifiziert unversorgte Gewerbegebiete (Flächennutzung) durch Verschneidung.
*   **Kiez-Analyse**: Aggregiert Versorgungslücken auf Bezirksebene, um unterversorgte Wohngegenden zu lokalisieren.

### 6. Visualization Phase (`steps/s06_visualization.py`)
Erstellt visuelle Repräsentationen der Analyseergebnisse:
*   **Strategie-Karte (PNG)**: Statische Karte mit Corporate-Identity-Farben für Präsentationen.
*   **Interaktive Web-Karte (HTML)**: Folium-basierte Karte mit Layer-Control (Telekom, Vodafone, Wettbewerb, Lücken, Geplant) und Choropleth-Darstellung der Bezirksversorgung.

---

## 📂 Directory Structure

```text
fiber_data/                # [NEW] Main Data Directory
│   ├── cache/             # Intermediate files (Tiles, Raw GPKGs)
│   ├── logs/              # Log files
│   └── output/            # Final Results (Maps, Master GPKG)
├── config.py              # Central configuration settings
├── main.py                # Unified entry point
├── steps/                 # Modular step package
│   ├── __init__.py
│   ├── s01_downloader.py
│   └── ...
└── requirements.txt       # Dependencies
```

---

## 🛠️ Installation

### Voraussetzungen
*   Python 3.9 oder höher
*   Empfohlen: Ein virtuelles Environment (`venv` oder `conda`)

### Setup

1.  **Repository klonen**
    ```bash
    git clone https://github.com/snennis/Anwendungsentwicklung_mit_Python.git
    cd Anwendungsentwicklung_mit_Python
    ```

2.  **Abhängigkeiten installieren**
    Die Analyse benötigt diverse GIS-Bibliotheken (GDAL, Rasterio, GeoPandas).
    ```bash
    pip install -r requirements.txt
    ```

> [!TIP]
> **Windows-Nutzer:** Falls die Installation von `fiona` oder `rasterio` fehlschlägt, nutzen Sie bitte vorkompilierte Wheels oder `conda install geopandas`.

---

## 💻 Nutzung

Die gesamte Pipeline wird über den zentralen Manager gesteuert. Dieser kümmert sich um Logging, Zeitmessung und Speicherbereinigung.

```bash
python main.py
```

### Konfiguration (`config.py`)
Möchten Sie den Analyse-Bereich, URLs oder Schwellenwerte ändern? Passen Sie einfach `config.py` im Stammverzeichnis an.

### Output
Die Ergebnisse sind strukturiert abgelegt:

*   **`fiber_data/output/`**:
    *   `05_master_analysis.gpkg`: Das finale GeoPackage mit allen Layern und Statistiken.
    *   `berlin_strategie_karte.png`: Statische Übersichtskarte.
    *   `berlin_interaktiv.html`: Interaktive Karte zur Detailanalyse.
*   **`fiber_data/logs/`**:
    *   `pipeline_run.log` und schrittspezifische Logs.
*   **`fiber_data/cache/`**:
    *   Rohdaten (Kacheln) und Zwischenergebnisse (für Debugging).

---

## 📊 Beispiel-Statistik (Auszug)

Das System generiert am Ende einen Report über die Flächennutzung:

| Status | Area (km²) |
| :--- | :--- |
| **Monopol Telekom** | 33.71 |
| **Monopol Vodafone** | 246.16 |
| **Wettbewerb** | 74.63 |
| **White Spot** | 531.65 |
| **Planung** | 13.17 |

---

## ⚠️ Disclaimer

> [!WARNING]
> Dieses Projekt dient ausschließlich wissenschaftlichen und bildenden Zwecken im Rahmen eines Geoinformatik-Studiums. Die Daten werden von öffentlichen Karten-Schnittstellen der Provider bezogen. Bitte beachten Sie die Nutzungsbedingungen der jeweiligen Diensteanbieter.