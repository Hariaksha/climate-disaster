# Attribution Index Codebook

**Project:** Reading the Storm — Media Attribution of U.S. Climate Disasters, 2000–2024
**Unit of analysis:** newspaper article (article-level codes), aggregated to disaster-event (disaster-level index)
**Inputs:** `attribution.xlsx`, sheet `Master` (1,694 candidate articles across 313 NOAA Billion-Dollar Disaster events)

---

## Overview

Coding proceeds in two sequential steps, applied to each row of the `Master` sheet:

1. **Relevance screening** (`Relevance` column): is this article actually about the
   disaster event it was retrieved for?
2. **Attribution coding** (`Attribution Index` column): for articles marked
   `Relevant`, what causal frame does the article apply to the event — anthropogenic
   climate change, natural/random variation, both, or neither?

Only `Relevant` articles receive an attribution code and enter the disaster-level
index. `Trash` and `Borderline` articles are excluded from the index (but retained
in the workbook for transparency/audit).

---

## Step 1 — Relevance Screening

| Code | Definition | Examples / cues |
|---|---|---|
| **Relevant** | Article substantively discusses *this specific* NOAA-listed event — correct disaster type, correct geography (state/region named in the `States` column), and a date consistent with the `Begin Date`/`End Date` window (including reasonable lead-up or retrospective coverage). | A news story on the storm's impacts, recovery, costs, forecasts, or causal framing. |
| **Borderline** | Plausibly about this event but ambiguous: a wire brief with too little detail to confirm event match, a "wildfire season" or "hurricane season" roundup that may or may not cover this specific event, or a retrospective piece (1+ years later) that references the event only in passing. | "Looking back at the 2018 hurricane season..." with one sentence on the target storm. |
| **Trash** | Off-topic, wrong event/year/disaster type, duplicate of another row, paywall/login/cookie-notice page, aggregator or archive listing page with no article content, dead link, or photo gallery (caption-only text with no article body — captions cannot contain sustained causal framing and thus cannot produce a meaningful attribution code). | A search result that is actually about a *different* hurricane; a homepage/section-front URL; a slideshow with only photo captions. |

**Note:** `Borderline` exists so that ambiguous cases are not forced into a binary
decision during the first pass; they can be revisited during calibration (Step 4
below) or excluded from the index by default, matching `Trash`.

---

## Step 2 — Attribution Coding (Relevant articles only)

Each relevant article is assigned one of five categories, which maps to a numeric
score $a_{i,d} \in [-1, 1]$ for article $i$ covering disaster $d$:

| Code | Label | Score | Definition | Example language |
|---|---|---|---|---|
| **A** | Explicit anthropogenic attribution | **+1.0** | Article directly and affirmatively links the event's occurrence, frequency, or intensity to human-caused climate change / global warming. Often cites climate scientists, formal attribution studies (e.g., World Weather Attribution), or uses confident causal language. | "Climate scientists say the unprecedented rainfall was supercharged by a warmer atmosphere, which holds more moisture." |
| **B** | Hedged / contextual climate mention | **+0.5** | Climate change is raised as a possible or partial factor, with hedged language ("may," "could," "some scientists believe"), or mentioned briefly/contextually without being central to the article's causal framing. | "The fire comes amid a years-long drought that some link to a changing climate." |
| **C** | No causal attribution | **0.0** | Pure event reporting — impacts, damages, casualties, response, recovery, forecasts — with no discussion of broader cause (climate or otherwise). | "The storm knocked out power to 500,000 homes across the Southeast." |
| **D** | Explicit non-climate attribution | **−1.0** | Article attributes the event to natural variability / cyclical weather patterns (El Niño/La Niña, "typical for this time of year," historical cycles) **or** to non-climate human factors (floodplain development, forest management, infrastructure failures) — and in doing so explicitly downplays or rejects a climate-change link. | "Meteorologists say this is a fairly typical La Niña winter pattern, not evidence of a longer-term trend." |
| **E** | Mixed / contested | **0.0** (flagged separately) | Article presents both climate-linked and non-climate framings with roughly comparable weight (e.g., a climate scientist and a skeptical official both quoted). | "While some researchers point to climate change as a contributing factor, local officials say the flooding was mainly due to outdated drainage infrastructure." |

### Auxiliary tags (do not affect the numeric score, but recorded for descriptive analysis)

- **D-subtype**: for category D, additionally note whether the non-climate
  attribution is to *natural variability* or to *other human causes* (land use,
  infrastructure, development). This lets the analysis distinguish "it's just
  weather" framings from "it's bad planning" framings, both of which read as −1
  on the climate-vs-natural axis but imply different policy narratives.
- **E flag (contested)**: category E scores 0, identically to category C, but is
  recorded as `Attribution Index = 0` with a separate `Contested = TRUE` flag (to
  be added alongside the index column) so that "no claim made" (C) is
  distinguishable from "claims actively dispute each other" (E) when computing
  disaster-level dispersion.
- **National Newspaper**: already computed in `Master` (column Q) from the
  National PSE domain list; used to test whether national vs. local outlets frame
  events differently.
- **Political lean**: planned addition — outlet-level lean (e.g., AllSides/Ad
  Fontes rating) joined by domain, to test whether attribution framing varies with
  outlet political orientation.

---

## Disaster-Level Attribution Index

For each disaster $d$ with $N_d$ relevant, coded articles:

$$
\text{AttributionIndex}_d = \frac{1}{N_d}\sum_{i=1}^{N_d} a_{i,d}, \qquad a_{i,d}\in\{-1,\,-0.5^{*},\,0,\,0.5,\,1\}
$$

($-0.5$ is reserved but not currently used by any category; the active values are
$\{-1, 0, 0.5, 1\}$ per the table above.)

Additional disaster-level fields computed alongside the index:

- $N_d$ — number of relevant, coded articles (already computed as `# Articles
  Found` will need to be re-derived as "# Relevant Articles" once relevance
  tagging is complete).
- **Contested share** — fraction of $N_d$ coded **E**.
- **No-coverage flag** — for the disasters with $N_d = 0$ (no relevant articles
  after screening, including the disasters that returned zero search results at
  all), `AttributionIndex_d` is left blank/NA rather than imputed. These
  disasters are analyzed separately as a "no media attribution" group rather than
  dropped silently.

---

## Calibration & Validation Procedure

To keep the LLM-assisted coding defensible, coding proceeds in stages rather than
as a single uncontrolled pass over all 1,694 rows. Two different reliability bars
are used at two different stages — a low, cheap **gate** during calibration to
catch codebook problems early, and a higher **reporting target** for the
independent audit that ends up in the paper.

1. **Human calibration sample.** The researcher hand-codes a random sample of
   ~50 articles (stratified across disaster types and time periods — pre-2010 vs.
   post-2010 — so both eras are represented) for both Relevance and Attribution
   Index, using this codebook.
2. **LLM pass on the calibration sample.** The same 50 articles are coded by the
   LLM using a prompt built directly from this codebook (category definitions +
   examples).
3. **Agreement check (gate).** Compare human vs. LLM codes using unweighted
   Cohen's $\kappa$ for the categorical codes (Relevance: 3 categories;
   Attribution: 5 categories). If $\kappa < 0.6$ on either dimension ("moderate"
   agreement or worse, per Landis \& Koch 1977), the codebook wording/examples are
   revised and the calibration sample is re-coded before scaling up. This 0.6
   threshold is intentionally low -- it exists to catch broken category
   definitions cheaply, not to certify reliability.
4. **Full-sample LLM pass.** Once the gate is cleared, run the LLM coder on all
   remaining articles in `Master`.
5. **Post-hoc audit sample (reporting target).** Draw an independent random
   sample (e.g., 100 articles, stratified) from the full coded set for a final
   human spot-check. Report, in the paper's Data/Methods section:
   - **Relevance (3-category):** unweighted Cohen's $\kappa$, targeting
     $\kappa \geq 0.7$ ("substantial" agreement).
   - **Attribution (5-category, A-E):** both unweighted $\kappa$ and a
     **weighted $\kappa$** (linear or quadratic weights) that reflects the
     ordinal structure of the underlying numeric scores
     ($-1 < 0 < 0.5 < 1$). Unweighted $\kappa$ on a 5-category scheme is a
     harsh metric -- a B-vs-C disagreement (score difference of 0.5) is
     penalized the same as an A-vs-D disagreement (score difference of 2) --
     so the weighted version is the more meaningful reliability measure here.
     Target $\kappa_w \geq 0.7$, ideally $\geq 0.8$.
   - **Mean absolute difference** between human- and LLM-assigned $a_{i,d}$
     scores on the audit sample, as a directly interpretable complement to
     $\kappa$ (e.g., "average discrepancy of 0.08 on a $[-1,1]$ scale").

   If the audit-sample statistics fall short of these targets, this should be
   disclosed and discussed as a limitation rather than treated as
   disqualifying -- the B/C and C/E category boundaries are genuinely fuzzy
   even for human coders, and a lower (but reported and discussed) weighted
   $\kappa$ is preferable to an inflated or unreported one.

This produces, for the paper: (a) a documented codebook, (b) reported
inter-rater reliability statistics (unweighted and weighted $\kappa$, plus mean
absolute score difference) from an independent audit sample, and (c) a fully
coded `Master` sheet from which `AttributionIndex_d` and `ContestedShare_d` are
computed via the formulas already wired into `attribution.xlsx` (`All Disasters`
sheet, columns K–L).
