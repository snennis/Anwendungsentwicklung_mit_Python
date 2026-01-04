# **📡 Fiber Optic Intelligence Platform**

Eine spezialisierte **Spatial ETL-Pipeline** zur Analyse der Glasfaser-Versorgungssituation (FTTH) in Berlin. Das System extrahiert Rasterdaten von Provider-Schnittstellen (WMS & ArcGIS REST), transformiert diese in bereinigte Vektorgeometrien und verschneidet sie mit administrativen Daten (WFS), um "White Spots" und Monopolstellungen präzise zu lokalisieren.

## **🚀 Key Features**

| Modul | Beschreibung |
| :---- | :---- |
| **⚡ Async Ingestion** | Hochperformanter Downloader via **AsyncIO & aiohttp**. Lädt >500 Kacheln in unter 40 Sekunden durch persistente Connections. |
| **🗺️ Vectorization & Clipping** | Transformation von Raster in Vektor & dynamischer Download der Stadtgrenze (OSMnx) für exaktes Clipping (keine rechteckige BBox mehr). |
| **🧹 Topology Cleaning** | Automatisierte Geometrie-Reparatur (Buffer-Dissolve-Unbuffer), um Artefakte zu entfernen und saubere Flächen für die Statistik zu gewährleisten. |
| **🧠 Spatial Analytics** | Mengenlehre-Operationen (Intersection, Difference) zur Ermittlung von Monopolen, Wettbewerbszonen und unversorgten Gebieten. |
| **🏙️ Context Enrichment** | Anreicherung der Daten durch WFS-Dienste (ALKIS Bezirke, Flächennutzung ISU5), um Lücken in Wohn- und Gewerbegebieten zu unterscheiden. |

## **🏗️ Architektur**

Das Projekt implementiert eine modulare Pipeline-Architektur mit strikter Trennung der Verantwortlichkeiten (SoC):

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

### **Die Pipeline-Schritte**

1. **Downloader (s01):** Erntet Kacheln asynchron (AsyncIO). Maximiert den Durchsatz durch Connection-Pooling, respektiert aber Server-Limits.
2. **Processor (s02):** Extrahiert Farbbereiche (z.B. Magenta für Telekom) aus den Bildern und konvertiert sie in Geometrien. Nutzt pyogrio für performantes Schreiben von GeoPackages.  
3. **Cleaning (s03):** Lädt die exakte Stadtgrenze via **OSMnx** und schneidet (Clips) die Daten passgenau zu. Bereinigt Artefakte und schließt Lücken durch Buffer-Operationen.
4. **Analysis (s04):** Berechnet Marktanteile und White Spots innerhalb der realen Stadtgrenze. Reprojiziert auf **EPSG:25833** für präzise Flächenberechnung. 
5. **Enrichment (s05):** Klassifiziert "White Spots" anhand der realen Nutzung (WFS 2021) in Vertriebs-Potenziale:
   * **High Potential:** Wohnen, Gewerbe, Mischgebiete (Priorität 1).
   * **Medium Potential:** Kleingärten, Sport, Kultur.
   * **Low Potential:** Wald, Wasser, Verkehrsflächen (wird in der Karte ausgeblendet).
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
   ```bash
   git clone https://github.com/snennis/Anwendungsentwicklung_mit_Python.git  
   cd Anwendungsentwicklung\_mit\_Python
   ```

2. **Environment aufsetzen**
   ```bash
   python -m venv venv
   # Windows
   .\venv\Scripts\activate
   # Linux / Mac
   source venv/bin/activate
   
   pip install -r requirements.txt
   ```
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

## **📊 Aktuelle Ergebnisse (Stand: 2026)**

### **Visualisierung der Versorgungslücken**
Die folgende Karte zeigt die identifizierten "White Spots", gefiltert nach ihrer wirtschaftlichen Relevanz (Wohnen/Gewerbe vs. Natur).

![Strategische Glasfaser-Karte Berlin](assets/berlin_strategie_karte.png)

---

### **1. Infrastruktur-Status (Gesamtfläche)**
Basis-Analyse der Netzabdeckung über das gesamte Stadtgebiet:

| Status | Fläche (km²) | Beschreibung |
| :---- | :---- | :---- |
| **Wettbewerb** | 74.62 | Infrastruktur beider Provider vorhanden |
| **Monopol Telekom** | 33.71 | Exklusive Versorgung durch Telekom |
| **Monopol Vodafone** | 246.14 | Exklusive Versorgung durch Vodafone (Coax/Fiber) |
| **Kein Netz (White Spot)** | 531.65 | Keine gigabitfähige Infrastruktur erkannt |

### **2. Vertriebs-Potenzial (Smart Analysis)**
Durch die Verschneidung mit Landnutzungsdaten (ISU5) wurde das **wirtschaftlich relevante Potenzial** pro Bezirk ermittelt.
* **High Potential:** Unversorgte Wohn- und Mischgebiete.
* **Mid Potential:** Unversorgte Kleingärten, Sport- und Kulturflächen.

| Bezirk | Versorgungsgrad (High) | 🔴 High Potential Gap (km²) | 🟡 Mid Potential Gap (km²) |
| :--- | :---: | :---: | :---: |
| **Treptow-Köpenick** | 58.9% | **16.22** | 7.84 |
| **Pankow** | 59.1% | **13.76** | 10.11 |
| **Spandau** | 61.3% | **12.49** | 6.28 |
| **Marzahn-Hellersdorf** | 67.6% | **10.29** | 2.17 |
| **Lichtenberg** | 56.4% | **9.86** | 5.52 |
| **Mitte** | 53.1% | **8.97** | 1.51 |
| **Tempelhof-Schöneberg** | 69.0% | **8.13** | 3.24 |
| **Steglitz-Zehlendorf** | 82.4% | **7.08** | 3.76 |
| **Reinickendorf** | 79.1% | **7.04** | 4.21 |
| **Neukölln** | 76.2% | **5.43** | 5.13 |
| **Charlottenburg-Wilm.** | 70.3% | **5.38** | 5.29 |
| **Friedrichshain-Kreuzberg** | 53.4% | **5.06** | 0.52 |

> **Fazit:** Während Innenstadtbezirke oft geometrisch gut versorgt wirken, zeigen Außenbezirke wie Treptow-Köpenick und Pankow den größten absoluten Nachholbedarf an relevanter Fläche.

## **⚠️ Disclaimer**

Dieses Projekt ist eine akademische Arbeit im Rahmen des Studiengangs Geoinformatik (B. Eng.). Die verwendeten Daten stammen aus öffentlichen Quellen (WMS/REST/WFS). Die Analyse stellt eine Momentaufnahme dar und dient ausschließlich Bildungszwecken.