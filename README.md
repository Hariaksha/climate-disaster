# 🌪️ Climate Disaster Media Coverage Dataset

A data pipeline for systematically collecting newspaper articles about U.S. billion-dollar weather and climate disasters (2000–2024), built on top of NOAA's Weather and Climate Billion-Dollar Disasters dataset.

---

## 📌 Overview

This project automates the discovery and collection of local newspaper articles covering major U.S. climate disasters — hurricanes, wildfires, floods, droughts, winter storms, freezes, and severe storms. It cross-references NOAA's official disaster event records with real-time web search to build a structured media coverage dataset suitable for economic and policy analysis.

**Primary use case:** Linking disaster-level economic damage data with local media coverage intensity — enabling research on public attention, political salience, and community-level climate risk perception.

---

## 📁 Repository Structure
- `code/`
  - `google_search.py` — Google Custom Search API pipeline (primary)
  - `newspaper_finder.py` — DuckDuckGo-based newspaper URL finder
  - `newspaper_finder_perplexity.py` — Perplexity API variant of newspaper finder
- `data/`
  - `events-US-2000-2024-Q4.csv` — NOAA billion-dollar disaster events    

---

## 🗃️ Data Source

The disaster events data is sourced from NOAA's **Weather and Climate Billion-Dollar Disasters** database:

> 📎 [NOAA Billion-Dollar Disasters (1980–2024)](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0209268)

- Coverage: U.S. weather events from 1980–2024 (pipeline targets **2000–2024**)
- Cost values are in **millions of USD**
- Disaster types: Tropical Cyclone, Wildfire, Flooding, Drought, Winter Storm, Freeze, Severe Storm

---

## ⚙️ Pipeline Overview

### `google_search.py` — Primary Search Engine
Uses the **Google Custom Search API** with multiple Custom Search Engine (CX) IDs to retrieve news article URLs for each disaster event.

Key features:
- Builds disaster-type-specific search queries (e.g., hurricane name + year + region)
- Filters results by publication date window (event dates ± grace period)
- Requires U.S. geographic relevance and disaster keyword matching
- Blocks non-article pages (sitemaps, author pages, tag pages, PDFs, homepages)
- Removes noise (sports, politics, international events)
- Deduplicates results by normalized URL
- Outputs up to 20 article URLs per disaster event
- Supports test mode (2 events per disaster type) and full production mode
- Auto-saves checkpoints every 25 disasters

### `newspaper_finder.py` — Newspaper Website Locator
Uses **DuckDuckGo Search** to find the official homepage URLs of local newspapers, given a spreadsheet of newspaper names, cities, and states.

Key features:
- Resume-safe (skips already-processed rows)
- Verifies URLs are live and not parked/redirected
- Blocks aggregators, archives, and social media domains
- Exponential backoff on rate limits with randomized delays
- Saves progress to Excel every 5 rows

### `newspaper_finder_perplexity.py`
Alternative version of the newspaper finder using the **Perplexity API** for higher-quality search results on ambiguous or hard-to-find newspapers.

---

## 🚀 Getting Started

### Prerequisites

```bash
pip install requests pandas openpyxl ddgs
```

For `google_search.py`, you'll also need a [Google Custom Search API key](https://developers.google.com/custom-search/v1/overview) and one or more CX engine IDs.

### Running the Google Search Pipeline

1. Set `INPUT_FILE` to your NOAA events CSV path
2. Set `TEST_MODE = True` for a quick trial run (14 sampled events)
3. Set `TEST_MODE = False` for a full production run

```bash
python code/google_search.py
```

### Running the Newspaper Finder

1. Set `SPREADSHEET_PATH` to your Excel file of newspaper names
2. Ensure columns are ordered: `Title | City | State | Language`

```bash
python code/newspaper_finder.py
```

---

## 📊 Output

| File | Description |
|------|-------------|
| `climate_disaster_article_urls.csv` | One row per disaster with up to 20 article URLs |
| `climate_disaster_checkpoint.csv` | Auto-saved checkpoint (every 25 disasters) |
| `Access World News Database.xlsx` | Newspaper URLs enriched with live status |

---

## 🔍 Search Quality Controls

The pipeline applies several filters to ensure only relevant, high-quality article URLs are collected:

- **Date window:** Articles must fall within the disaster event period + a scaled grace period (14–42 days)
- **Geographic filter:** Requires at least one U.S. state, region, or institutional term
- **Disaster relevance:** Keyword match in title, or 2+ matches across title and snippet
- **Geo-specificity:** If a disaster name contains a known place, that place must appear in the article
- **Noise filter:** Rejects articles matching off-topic phrases (sports, elections, international events)

---

## 📄 License

Data sourced from NOAA is publicly available. Code in this repository is for research purposes.

---

*Built as part of ongoing research on climate economics and media salience.*
