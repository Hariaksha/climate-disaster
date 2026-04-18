# 🌪️ Climate Disaster Attribution & Sentiment Analysis

A research project combining a large-scale media coverage dataset with natural language processing (NLP) and sentiment analysis to study how U.S. climate disasters are attributed in newspaper coverage — and how attribution patterns vary across geography, disaster type, and other dimensions.

Built on top of NOAA's Weather and Climate Billion-Dollar Disasters dataset.

> **Collaborators:** Hari Gunda, Dr. Michael Price (University of Alabama), Dr. Angela Doku (University of Toronto), Dr. John List (University of Chicago)

---

## 📌 Overview

This project investigates how local newspaper coverage of major U.S. climate disasters frames the *cause* of those disasters. Specifically, we ask:

> **Are climate disasters attributed to man-made causes and climate change, or to natural and random variation — and what factors drive differences in attribution?**

To answer this question, the project proceeds in two stages:

1. **Data collection:** A pipeline that systematically scrapes and collects newspaper articles covering NOAA-documented U.S. billion-dollar climate disasters (2000–2024).
2. **NLP & sentiment analysis:** Applying natural language processing models to classify the causal framing of each article and construct an **Attribution Index** at both the article and disaster level.

---

## 🔬 Research Design

### Stage 1 — Data Collection

The pipeline cross-references NOAA's official disaster event records with real-time web search to build a structured media coverage dataset. Disaster types include tropical cyclones, wildfires, floods, droughts, winter storms, freezes, and severe storms.

### Stage 2 — NLP & Attribution Analysis

Each collected article is processed using NLP to classify its causal framing along a spectrum:

| Attribution Type | Description |
|---|---|
| **Anthropogenic** | Explicitly links disaster to climate change, human activity, fossil fuels, or man-made causes |
| **Natural / Random** | Frames disaster as a natural, random, or historically normal event |
| **Mixed / Neutral** | No clear causal attribution or balanced framing |

From these classifications, we construct an **Attribution Index** — a continuous measure of anthropogenic vs. natural causal framing — at the:
- **Article level** (individual piece of coverage)
- **Disaster event level** (aggregated across all articles about a given disaster)

### Stage 3 — Variation in Attribution

We then examine how the Attribution Index varies across:

- **Geography (state):** Do newspapers in coastal states attribute disasters differently than inland states? Do politically red vs. blue states differ?
- **Disaster type:** Are wildfires and hurricanes more likely to be attributed to climate change than floods or severe storms?
- **Disaster severity:** Does damage magnitude (in USD) predict anthropogenic framing?
- **Time:** Has attribution shifted between 2000 and 2024 as climate science has entered mainstream discourse?
- **Newspaper characteristics:** Do national outlets differ from local papers? Do circulation size or regional identity matter?

---

## 📁 Repository Structure
- `code/`
  - `google_search.py` — Google Custom Search API pipeline (primary)
  - `newspaper_finder.py` — DuckDuckGo-based newspaper URL finder
  - `newspaper_finder_perplexity.py` — Perplexity API variant of newspaper finder
- `data/`
  - `events-US-2000-2024-Q4.csv` — NOAA billion-dollar disaster events
  - `Access World News Database.xlsx` - Newspaper URLs enriched with live status
- `goals.docx` - Project goals and research design notes
- `updates.docx` - Progress updates
- `README.md`


---

## 🗃️ Data Sources

**Disaster events:** NOAA's Weather and Climate Billion-Dollar Disasters database
> 📎 [NOAA Billion-Dollar Disasters (1980–2024)](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0209268)

- Coverage: U.S. weather events from 1980–2024 (pipeline targets **2000–2024**)
- Cost values are in **millions of USD**
- Disaster types: Tropical Cyclone, Wildfire, Flooding, Drought, Winter Storm, Freeze, Severe Storm

**Media coverage:** Newspaper articles collected via Google Custom Search API and the Access World News Database, targeting local and regional U.S. newspapers.

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

For `google_search.py`, you will also need a [Google Custom Search API key](https://developers.google.com/custom-search/v1/overview) and one or more CX engine IDs.

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
|---|---|
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

## 💡 Applications

The Attribution Index and underlying dataset have broad applications across research, policy, and communication:

- **Climate communication research:** Understand how media framing of disasters has evolved over time and whether attribution language has tracked the scientific consensus on climate change.
- **Public perception & risk modeling:** Identify regions where anthropogenic attribution is systematically low, which may signal gaps in public climate risk awareness and inform targeted communication strategies.
- **Political economy of climate policy:** Examine whether local media attribution patterns correlate with state-level climate policy adoption, electoral outcomes, or public opinion survey data.
- **Insurance & financial risk:** Help insurers and financial institutions understand regional media environments that may affect policyholder behavior and willingness to pay for climate risk products.
- **Disaster preparedness & public health:** Inform agencies like FEMA and state emergency management offices about how disaster framing affects community preparedness and response behavior.
- **Nonprofit & advocacy strategy:** Enable environmental organizations to identify media markets where attributional framing is weakest and where targeted outreach may be most impactful.
- **Academic benchmarking:** Provide a replicable methodology and open dataset for future NLP studies on climate media, political communication, and environmental journalism.

---

## 📄 License

Data sourced from NOAA is publicly available. Code in this repository is for research purposes.

Built as part of ongoing research on climate economics and media salience at the University of Alabama.
