# 🌪️ Reading the Storm: Media Attribution of U.S. Climate Disasters, 2000–2024

An empirical paper and supporting codebase studying how U.S. newspaper coverage frames the causal origins of major climate disasters — and how attribution patterns vary across outlets, time, and geography.

Built on top of NOAA's Weather and Climate Billion-Dollar Disasters dataset and a validated LLM-based coding pipeline.

> **Authors:** Hariaksha Gunda (University of Alabama), Dr. Michael Price (University of Alabama), Dr. Angela Doku (University of Toronto), Dr. John List (University of Chicago)

---

## 📌 Overview

This project asks: when a hurricane makes landfall or a drought devastates a region, does U.S. newspaper coverage attribute the disaster to anthropogenic climate change, to natural variability, or to nothing at all?

To answer this, we construct an **Attribution Index** measuring the causal framing of 1,252 newspaper articles covering all 313 NOAA billion-dollar disaster events from 2000 to 2024. Article-level scores are aggregated to a disaster-level index and analyzed with panel regressions.

> **Paper:** *Reading the Storm: Media Attribution of United States Climate Disasters, 2000–2024* — see `paper/main.pdf`

---

## 🔬 Key Findings

1. **Silence dominates.** 74.8% of articles contain no causal claim. The mean disaster-level Attribution Index is 0.028 — close to neutral — and 55.5% of covered disasters receive an index of exactly 0.

2. **Attribution has risen over time.** Framing has shifted at 0.013 index points per year (*p* < 0.001), from a pre-2010 mean of −0.105 to a post-2010 mean of +0.057, consistent with a structural break in partisan climate discourse around 2010–2011.

3. **Outlet political lean predicts attribution.** A one-point increase in Ad Fontes Bias score is associated with a 0.011-point decrease in the Attribution Index (*p* = 0.013, clustered SE). This effect is concentrated in national and wire-service coverage and is robust to disaster fixed effects and alternative lean measures.

4. **Physical disaster characteristics do not predict attribution.** Disaster type, CPI-adjusted cost, death toll, duration, and geography are all statistically indistinguishable from zero.

---

## 📁 Repository Structure

```
climate-disaster/
├── paper/
│   ├── main.tex                   # LaTeX source
│   ├── main.pdf                   # Compiled paper (40 pages)
│   ├── references.bib             # BibTeX bibliography (17 entries)
│   ├── attribution_codebook.md    # LLM coding codebook
│   └── figures/                   # Paper figures
├── literature/
│   ├── literature-review.xlsx     # Annotated bibliography (17 papers)
│   └── *.pdf                      # Source PDFs for all cited papers
├── code/
│   ├── google_search.py           # Google Custom Search API pipeline
│   ├── llm_coder.py               # LLM attribution coding pipeline
│   ├── audit_reliability.py       # Inter-rater reliability statistics
│   ├── sample_audit.py            # Audit sample selection
│   ├── Stage3_Regressions.ipynb   # Regression analysis
│   ├── newspaper_finder.py        # Newspaper URL finder (DuckDuckGo)
│   ├── newspaper_finder_perplexity.py  # Newspaper URL finder (Perplexity API)
│   └── build_state_engine_mapping.py
├── data/
│   ├── events-US-2000-2024-Q4.csv          # NOAA billion-dollar disaster events
│   ├── events-US-2000-2024-Q4-states.csv   # Events enriched with affected states
│   ├── Access World News Database.xlsx      # Newspaper URLs with live status
│   ├── pse_domains.json                     # Programmable Search Engine domains
│   └── pse_coverage_analysis.json
├── attribution.xlsx               # Article-level and disaster-level Attribution Index
└── climate_disaster_article_urls.csv  # Retrieved article URLs (up to 20 per disaster)
```

---

## 📊 Attribution Index

Each article is classified on a five-point scale following a written codebook and validated against an independent human audit (κ = 0.96 for relevance, weighted κ_w = 0.96–0.98 for attribution):

| Code | Score | Description |
|------|-------|-------------|
| A | +1 | Explicit anthropogenic attribution (links disaster to climate change or human activity) |
| B | +0.5 | Hedged or contextual climate mention |
| C | 0 | No causal attribution (pure event reporting) |
| D | −1 | Explicit non-climate / natural-variability attribution |
| M | 0† | Mixed or contested framing (flagged separately) |

Article-level scores are averaged to produce a **disaster-level Attribution Index** ∈ [−1, +1].

---

## 🗃️ Data Sources

**Disaster events:** NOAA's Weather and Climate Billion-Dollar Disasters database
> 📎 [NOAA Billion-Dollar Disasters (1980–2024)](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0209268)

- Coverage: U.S. events 2000–2024 (313 events meeting billion-dollar threshold in real 2024 dollars)
- Disaster types: Tropical Cyclone, Wildfire, Flooding, Drought, Winter Storm, Freeze, Severe Storm

**Media coverage:** Newspaper articles retrieved via Google Custom Search API (multiple Programmable Search Engine CX IDs targeting U.S. newspaper domains).

**Outlet lean ratings:** Ad Fontes Media bias scores (and AllSides as a robustness check), matched to outlet domains.

---

## ⚙️ Pipeline Overview

### `google_search.py` — Article Retrieval

Uses the Google Custom Search API to retrieve news article URLs for each NOAA disaster event.

Key features:
- Builds disaster-type-specific search queries (e.g., hurricane name + year + region)
- Filters results by publication date window (event dates ± scaled grace period of 14–42 days)
- Requires U.S. geographic relevance and disaster keyword matching
- Blocks non-article pages (sitemaps, author pages, tag pages, PDFs, homepages)
- Removes noise (sports, politics, international events)
- Deduplicates results by normalized URL
- Outputs up to 20 article URLs per disaster event
- Auto-saves checkpoints every 25 disasters

### `llm_coder.py` — Attribution Coding

Applies a large language model (Claude, Anthropic) to classify each article against the written codebook in two passes: (1) relevance screening and (2) attribution category assignment.

### `audit_reliability.py` — Reliability Statistics

Computes inter-rater reliability (Cohen's κ and linearly weighted κ_w) between LLM codes and independent human audit codes.

### `newspaper_finder.py` / `newspaper_finder_perplexity.py` — Newspaper URL Discovery

Finds official homepage URLs for local newspapers given a list of names, cities, and states. DuckDuckGo and Perplexity API variants.

### `Stage3_Regressions.ipynb` — Analysis

Disaster-level and article-level OLS regressions with heteroskedasticity-robust (HC1) and clustered standard errors. Includes robustness checks: ordered logit, disaster fixed effects, alternative lean measures, subsample splits.

---

## 🚀 Getting Started

### Prerequisites

```bash
pip install requests pandas openpyxl ddgs anthropic
```

For `google_search.py`, you will also need a [Google Custom Search API key](https://developers.google.com/custom-search/v1/overview) and one or more CX engine IDs.

### Running the Search Pipeline

```bash
python code/google_search.py
```

Set `TEST_MODE = True` for a quick trial run (14 sampled events) or `False` for the full production run.

### Running the LLM Coder

```bash
python code/llm_coder.py
```

Reads article URLs from `climate_disaster_article_urls.csv`, fetches article text, and outputs attribution codes to `attribution.xlsx`.

---

## 📄 License

Data sourced from NOAA is publicly available. Code in this repository is for research purposes.

Built as part of ongoing research on climate economics and media salience at the University of Alabama.
