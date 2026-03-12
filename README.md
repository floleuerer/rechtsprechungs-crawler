# Rechtsprechungs-Crawler

Crawler-Sammlung zum automatisierten Herunterladen von Gerichtsentscheidungen deutscher Gerichte und Landesrechtsprechungsdatenbanken.

---

> **WICHTIGER HINWEIS / HAFTUNGSAUSSCHLUSS**
>
> Die Nutzung dieser Software erfolgt **auf eigene Verantwortung**. Vor dem Einsatz eines Crawlers **muss** geprüft werden, ob das Crawlen der jeweiligen Website rechtlich zulässig ist. Dazu gehört insbesondere:
>
> - Prüfung der **robots.txt** der Zielseite
> - Prüfung der **Nutzungsbedingungen** des jeweiligen Portals
> - Einhaltung des **Urheberrechts** und des **Datenbankherstellerrechts** (§§ 87a ff. UrhG)
> - Beachtung der **DSGVO**, sofern personenbezogene Daten betroffen sind
> - Einhaltung von **Rate-Limits** und angemessenen Abständen zwischen Anfragen
>
> Der Crawler identifiziert sich standardmäßig transparent als `LegalCrawl/0.1 (legal research crawler)`. Dieses Verhalten sollte beibehalten werden, um den Betreibern der Zielseiten eine Identifikation zu ermöglichen.
>
> **Die Autoren übernehmen keinerlei Haftung für Schäden, die durch die Nutzung dieser Software entstehen.**

---

## Übersicht

LegalCrawl besteht aus zwei Gruppen von Crawlern:

- **`common_crawl/`** – Crawler für Landesrechtsprechungsdatenbanken, die auf einer gemeinsamen Portalstruktur basieren (Selenium-basiert, gemeinsame Basisklasse `BaseCommonCrawler`)
- **`custom_crawl/`** – Crawler für Portale mit individueller Struktur (teils Selenium-, teils requests-basiert)

### Unterstützte Gerichte / Portale

| Portal | Crawler | Link | Typ |
|---|---|---|---|
| Bundesverfassungsgericht | `custom_crawl/bverfg_crawler.py` | [bundesverfassungsgericht.de](https://www.bundesverfassungsgericht.de) | requests |
| Baden-Württemberg | `common_crawl/bw_crawler.py` | [landesrecht-bw.de](https://www.landesrecht-bw.de) | Selenium |
| Bayern | `custom_crawl/bayern_crawler.py` | [gesetze-bayern.de](https://www.gesetze-bayern.de) | requests |
| Berlin | `common_crawl/berlin_crawler.py` | [gesetze.berlin.de](https://gesetze.berlin.de) | Selenium |
| Brandenburg | `custom_crawl/brandenburg_crawler.py` | [gerichtsentscheidungen.brandenburg.de](https://gerichtsentscheidungen.brandenburg.de) | Selenium |
| Hamburg | `common_crawl/hamburg_crawler.py` | [landesrecht-hamburg.de](https://www.landesrecht-hamburg.de) | Selenium |
| Hessen | `common_crawl/hessen_crawler.py` | [lareda.hessenrecht.hessen.de](https://lareda.hessenrecht.hessen.de) | Selenium |
| Mecklenburg-Vorpommern | `common_crawl/mv_crawler.py` | [landesrecht-mv.de](https://www.landesrecht-mv.de) | Selenium |
| Nordrhein-Westfalen | `custom_crawl/nrw_crawler.py` | [nrwesuche.justiz.nrw.de](https://nrwesuche.justiz.nrw.de) | Selenium |
| Rheinland-Pfalz | `common_crawl/rlp_crawler.py` | [landesrecht.rlp.de](https://landesrecht.rlp.de) | Selenium |
| Saarland | `common_crawl/saarland_crawler.py` | [recht.saarland.de](https://recht.saarland.de) | Selenium |
| Sachsen-Anhalt | `common_crawl/sachsen_anhalt_crawler.py` | [landesrecht.sachsen-anhalt.de](https://www.landesrecht.sachsen-anhalt.de) | Selenium |
| Schleswig-Holstein | `common_crawl/sh_crawler.py` | [sh.juris.de](https://www.sh.juris.de) | Selenium |
| Thüringen | `common_crawl/thueringen_crawler.py` | [landesrecht-thueringen.de](https://www.landesrecht-thueringen.de) | Selenium |

> **Hinweis:** Crawler für **Bremen**, **Niedersachsen** und **Sachsen** sind noch nicht enthalten, werden aber nachgeliefert.

## Installation

### Voraussetzungen

- Python >= 3.12
- [uv](https://github.com/astral-sh/uv) (empfohlen) oder pip
- Google Chrome (für Selenium-basierte Crawler)

### Setup

```bash
git clone https://github.com/floleuerer/rechtsprechungs-crawler
cd rechtsprechungs-crawler

# Mit uv (empfohlen)
uv sync

# Oder mit pip
pip install -e .
```

Für Selenium-basierte Crawler wird automatisch der passende ChromeDriver über `webdriver-manager` heruntergeladen. Zusätzlich müssen Selenium-Abhängigkeiten installiert sein:

```bash
uv pip install selenium webdriver-manager
```

## Nutzung

Alle Crawler teilen eine einheitliche CLI-Schnittstelle:

```bash
uv run python common_crawl/berlin_crawler.py [OPTIONEN]
# oder
uv run python custom_crawl/bayern_crawler.py [OPTIONEN]
```

### CLI-Optionen

| Option | Beschreibung | Standard |
|---|---|---|
| `--limit N` | Maximale Anzahl herunterzuladender Entscheidungen | Keine (alle) |
| `--no-headless` | Browser-Fenster sichtbar anzeigen (nur Selenium-Crawler) | Headless |
| `--overwrite` | Bereits vorhandene Dateien überschreiben | Aus |
| `--stealth` | Browser-ähnlichen User-Agent verwenden statt Crawler-UA | Aus |
| `--skip-threshold N` | Nach N aufeinanderfolgenden bereits vorhandenen Dateien stoppen | 50 |
| `--output PFAD` | Ausgabeverzeichnis | `data/<name>_raw` |

### Beispiele

```bash
# 100 Entscheidungen aus Berlin crawlen
uv run python common_crawl/berlin_crawler.py --limit 100

# Bayern-Crawler mit sichtbarem Browser
uv run python custom_crawl/bayern_crawler.py --no-headless --limit 10

# Inkrementeller Crawl (Standard): stoppt nach 50 bereits vorhandenen Dateien
uv run python common_crawl/hamburg_crawler.py

# Alles neu crawlen (überschreibt vorhandene Dateien, kein Skip-Threshold)
uv run python common_crawl/hamburg_crawler.py --overwrite --skip-threshold 0

# Ausgabe in anderes Verzeichnis
uv run python custom_crawl/bverfg_crawler.py --output /tmp/bverfg_data
```

## Inkrementeller Crawl

Standardmäßig prüft jeder Crawler vor dem Herunterladen, ob die Zieldatei bereits existiert. Wird eine Entscheidung übersprungen, zählt ein interner Zähler hoch. Nach 50 aufeinanderfolgenden Übersprüngen (konfigurierbar über `--skip-threshold`) stoppt der Crawler automatisch. So können regelmäßig nur neue Entscheidungen heruntergeladen werden, ohne den gesamten Bestand erneut zu crawlen.

Mit `--skip-threshold 0` wird dieses Verhalten deaktiviert.

## Ausgabeformat

Jede Entscheidung wird als JSON-Datei gespeichert:

```json
{
  "url": "https://...",
  "crawled_at": 1710000000.0,
  "metadata": {
    "title": "...",
    "Aktenzeichen": "...",
    "Gericht": "..."
  },
  "content": "Volltext der Entscheidung...",
  "html_content": "<main>...</main>"
}
```

Die verfügbaren Metadaten-Felder variieren je nach Portal.

## Projektstruktur

```
legalcrawl/
├── common_crawl/
│   ├── base_crawler.py          # Gemeinsame Basisklasse (BaseCommonCrawler)
│   ├── berlin_crawler.py        # Berlin
│   ├── bw_crawler.py            # Baden-Württemberg
│   ├── hamburg_crawler.py       # Hamburg
│   ├── hessen_crawler.py        # Hessen
│   ├── mv_crawler.py            # Mecklenburg-Vorpommern
│   ├── rlp_crawler.py           # Rheinland-Pfalz
│   ├── saarland_crawler.py      # Saarland
│   ├── sachsen_anhalt_crawler.py # Sachsen-Anhalt
│   ├── sh_crawler.py            # Schleswig-Holstein
│   └── thueringen_crawler.py    # Thüringen
├── custom_crawl/
│   ├── bayern_crawler.py        # Bayern (requests)
│   ├── brandenburg_crawler.py   # Brandenburg (Selenium)
│   ├── bverfg_crawler.py        # Bundesverfassungsgericht (requests)
│   └── nrw_crawler.py           # Nordrhein-Westfalen (Selenium)
├── pyproject.toml
└── README.md
```

## Lizenz

MIT License — siehe [LICENSE](LICENSE).
