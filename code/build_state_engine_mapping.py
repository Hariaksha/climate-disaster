"""
Build a domain -> state -> PSE engine mapping, and assign each NOAA disaster
event a set of likely affected states (and the corresponding PSE engines that
should be queried for it).

Inputs:
  - data/pse_domains.json              (engine -> {cx, domains[]})
  - data/Access World News Database.xlsx (verified newspaper domains -> State)
  - data/events-US-2000-2024-Q4.csv     (NOAA billion-dollar disaster events)

Outputs:
  - data/state_engine_map.json          (state -> sorted list of engine names)
  - data/events-US-2000-2024-Q4-states.csv (events + State/States + Engines columns)

Methodology notes (documented for the research write-up):
  - NOAA's per-event "states affected" data could not be fetched (web access
    to ncei.noaa.gov was unavailable in this environment), so states are
    inferred from the event "Name" field using:
      1. Explicit state/city names already present in the event name
         (e.g. "Houston Flooding" -> TX, "Colorado Hail Storms" -> CO).
      2. Directional/regional descriptors (e.g. "Southeast", "Plains",
         "Ohio Valley") mapped to the states in REGION_TO_STATES below.
         These mappings follow standard US Census / NOAA climate-region
         conventions, deliberately erring broad (a region term contributes
         its full state list; multiple matched terms are unioned).
      3. Named tropical cyclones / generic events with no geographic term
         in the name are assigned states from documented
         landfall/major-impact areas (HURRICANE_STATES below), based on
         general historical knowledge of each storm.
      4. Anything left unresolved is tagged "National" (no state-specific
         restriction -> falls back to using the National engine + all
         engines whose covered states intersect nothing specific).

  This is a best-effort, documented heuristic, not ground truth. It should be
  reviewed before being used as a research variable (e.g. for the
  geography-based attribution analysis in Stage 3). It is primarily intended
  to drive *search engine selection* in google_search.py (Task #8), where
  being a bit too broad is safe (more engines queried) but being too narrow
  risks missing relevant local coverage.
"""

import json
import re
from collections import defaultdict

import pandas as pd

PSE_DOMAINS_PATH = "data/pse_domains.json"
AWN_PATH = "data/Access World News Database.xlsx"
EVENTS_PATH = "data/events-US-2000-2024-Q4.csv"

OUT_STATE_ENGINE_MAP = "data/state_engine_map.json"
OUT_EVENTS = "data/events-US-2000-2024-Q4-states.csv"


# ---------------------------------------------------------------------------
# Region / state name -> list of US state abbreviations
# Order matters: more specific multi-word terms are listed first so they are
# checked (and reported) before generic single-word terms.
# ---------------------------------------------------------------------------
REGION_TO_STATES = [
    ("south florida", ["FL"]),
    ("fort lauderdale", ["FL"]),
    # State names whose generic "North"/"South" qualifier would otherwise be
    # caught by the standalone "south"/"southern" terms below (e.g. "South
    # Carolina"/"South Dakota" contain the standalone word "south"). Listed
    # early so they're matched and consumed before "south"/"southern" runs.
    ("south dakota", ["SD"]),
    ("north dakota", ["ND"]),
    ("south carolina", ["SC"]),
    ("north carolina", ["NC"]),
    ("east coast", ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "NJ", "PA", "DE", "MD", "VA", "WV", "NC", "SC", "GA", "FL"]),
    ("great plains", ["ND", "SD", "NE", "KS", "OK", "TX", "MT", "WY", "CO"]),
    ("upper midwest", ["IA", "MI", "MN", "WI", "ND", "SD"]),
    ("north central", ["ND", "SD", "MN", "IA", "NE", "WI", "MO", "IL", "IN", "MI", "OH", "KS"]),
    ("ohio valley", ["IL", "IN", "KY", "MO", "OH", "TN", "WV"]),
    ("mid-atlantic", ["NY", "NJ", "PA", "DE", "MD", "VA", "WV", "DC"]),
    ("new mexico", ["NM"]),
    ("northeastern", ["CT", "ME", "MA", "NH", "NJ", "NY", "PA", "RI", "VT"]),
    ("northeast", ["CT", "ME", "MA", "NH", "NJ", "NY", "PA", "RI", "VT"]),
    ("northwestern", ["WA", "OR", "ID", "MT"]),
    ("northwest", ["WA", "OR", "ID", "MT"]),
    ("southeastern", ["AL", "FL", "GA", "KY", "MS", "NC", "SC", "TN", "VA", "WV"]),
    ("southeast", ["AL", "FL", "GA", "KY", "MS", "NC", "SC", "TN", "VA", "WV"]),
    ("southwest", ["AZ", "NM", "CO", "UT", "NV", "TX"]),
    ("southern", ["AL", "AR", "FL", "GA", "KY", "LA", "MS", "NC", "OK", "SC", "TN", "TX", "VA"]),
    ("south", ["AL", "AR", "FL", "GA", "KY", "LA", "MS", "NC", "OK", "SC", "TN", "TX", "VA"]),
    ("rockies", ["CO", "WY", "MT", "ID", "UT"]),
    ("plains", ["ND", "SD", "NE", "KS", "OK", "TX", "MT", "WY", "CO"]),
    ("midwest", ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI"]),
    ("center", ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI", "OK", "TX"]),
    ("central", ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI", "OK", "TX"]),
    ("eastern", ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "NJ", "PA", "DE", "MD", "VA", "WV", "NC", "SC", "GA", "FL"]),
    ("east", ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "NJ", "PA", "DE", "MD", "VA", "WV", "NC", "SC", "GA", "FL"]),
    ("western", ["CA", "NV", "OR", "WA", "ID", "AZ", "UT", "CO", "NM", "MT", "WY", "AK", "HI"]),
    ("west", ["CA", "NV", "OR", "WA", "ID", "AZ", "UT", "CO", "NM", "MT", "WY", "AK", "HI"]),
    ("northern", ["MT", "ND", "MN", "WI", "MI", "NY", "VT", "NH", "ME", "ID", "WA"]),
    # explicit single states / cities
    ("florida", ["FL"]),
    ("texas", ["TX"]),
    ("houston", ["TX"]),
    ("california", ["CA"]),
    ("kentucky", ["KY"]),
    ("missouri", ["MO"]),
    ("hawaii", ["HI"]),
    ("arizona", ["AZ"]),
    ("illinois", ["IL"]),
    ("louisiana", ["LA"]),
    ("arkansas", ["AR"]),
    ("minnesota", ["MN"]),
    ("georgia", ["GA"]),
    ("colorado", ["CO"]),
    ("mississippi", ["MS"]),
    ("united states", ["NATIONAL"]),
    ("u.s.", ["NATIONAL"]),
]

# Named tropical cyclones / generic events with no geographic term in the
# "Name" field. Mapped to documented landfall / major-impact US states
# (territories: PR = Puerto Rico, GU = Guam).
HURRICANE_STATES = {
    "Tropical Storm Allison (June 2001)": ["TX", "LA"],
    "Hurricane Lili (October 2002)": ["LA"],
    "Tropical Storm Isidore (September 2002)": ["LA", "MS", "AL"],
    "Hurricane Isabel (September 2003)": ["NC", "VA", "MD", "DC", "PA", "NJ", "DE", "WV", "NY"],
    "Hurricane Charley (August 2004)": ["FL", "NC", "SC"],
    "Hurricane Frances (September 2004)": ["FL", "GA", "SC", "NC"],
    "Hurricane Ivan (September 2004)": ["AL", "FL", "MS", "GA"],
    "Hurricane Jeanne (September 2004)": ["FL"],
    "Hurricane Dennis (July 2005)": ["FL", "AL", "MS"],
    "Hurricane Katrina (August 2005)": ["LA", "MS", "AL", "FL"],
    "Hurricane Rita (September 2005)": ["TX", "LA"],
    "Hurricane Wilma (October 2005)": ["FL"],
    "Hurricane Dolly (July 2008)": ["TX"],
    "Hurricane Gustav (September 2008)": ["LA"],
    "Hurricane Ike (September 2008)": ["TX", "LA"],
    "Hurricane Irene (August 2011)": ["NC", "VA", "MD", "NJ", "NY", "CT", "VT", "MA", "NH", "ME"],
    "Tropical Storm Lee (September 2011)": ["LA", "PA", "NY"],
    "Hurricane Isaac (August 2012)": ["LA", "MS"],
    "Hurricane Sandy (October 2012)": ["NJ", "NY", "CT", "PA", "MD", "DE", "VA", "RI", "MA", "NH", "WV", "OH"],
    "Hurricane Matthew (October 2016)": ["FL", "GA", "SC", "NC"],
    "Hurricane Harvey (August 2017)": ["TX", "LA"],
    "Hurricane Irma (September 2017)": ["FL", "GA", "SC"],
    "Hurricane Maria (September 2017)": ["PR"],
    "Hurricane Florence (September 2018)": ["NC", "SC"],
    "Hurricane Michael (October 2018)": ["FL", "GA", "AL"],
    "Hurricane Dorian (September 2019)": ["NC", "SC"],
    "Tropical Storm Imelda (September 2019)": ["TX"],
    "Hurricane Hanna (July 2020)": ["TX"],
    "Hurricane Isaias (August 2020)": ["FL", "NC", "SC", "NY", "NJ", "CT", "PA", "MA"],
    "Hurricane Laura (August 2020)": ["LA", "TX"],
    "Hurricane Sally (September 2020)": ["AL", "FL"],
    "Hurricane Delta (October 2020)": ["LA"],
    "Hurricane Zeta (October 2020)": ["LA", "MS", "AL", "GA"],
    "Tropical Storm Eta (November 2020)": ["FL"],
    "Louisiana Flooding (May 2021)": ["LA"],
    "Tropical Storm Elsa (July 2021)": ["FL", "GA", "SC", "NC"],
    "Tropical Storm Fred (August 2021)": ["FL", "AL", "GA", "NC"],
    "Hurricane Ida (August 2021)": ["LA", "MS", "NJ", "NY", "PA", "CT"],
    "Hurricane Nicholas (September 2021)": ["TX", "LA"],
    "Hurricane Fiona (September 2022)": ["PR"],
    "Hurricane Ian (September 2022)": ["FL", "SC"],
    "Hurricane Nicole (November 2022)": ["FL"],
    "Typhoon Mawar (May 2023)": ["GU"],
    "Hurricane Idalia (August 2023)": ["FL", "GA", "SC", "NC"],
    "Hurricane Beryl (July 2024)": ["TX"],
    "Hurricane Debby (August 2024)": ["FL", "GA", "SC", "NC"],
    "Hurricane Francine (September 2024)": ["LA"],
    "Hurricane Helene (September 2024)": ["FL", "GA", "SC", "NC", "TN", "VA"],
    "Hurricane Milton (October 2024)": ["FL"],
    # explicit-place events not caught by REGION_TO_STATES
    "Illinois Flooding and Severe Weather (April 2013)": ["IL"],
    "Houston Flooding (April 2016)": ["TX"],
    "Louisiana Flooding (August 2016)": ["LA"],
    "Arkansas River Flooding (June 2019)": ["AR"],
    "Minnesota Hail Storms (August 2023)": ["MN"],
    # "Oklahoma"/"Kansas"/"Michigan"/"Alaska" have no REGION_TO_STATES terms,
    # so these multi-state event names would otherwise resolve to only the
    # one state ("texas"/"california") that does have a term.
    "Oklahoma, Kansas, and Texas Tornadoes and Severe Weather (May 2010)": ["KS", "OK", "TX"],
    "Michigan and Northeast Flooding (August 2014)": ["CT", "DE", "FL", "GA", "MA", "MD", "ME", "MI", "NC", "NH", "NJ", "NY", "PA", "RI", "SC", "VA", "VT", "WV"],
    "Texas and Oklahoma Flooding and Severe Weather (May 2015)": ["OK", "TX"],
    "California and Alaska Wildfires (Summer-Fall 2019)": ["AK", "CA"],
    "Texas and Oklahoma Severe Weather (April 2021)": ["OK", "TX"],
    "Center Severe Weather (May 2014)": ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI", "OK", "TX"],
    # "South Dakota" substring otherwise matches the "south"/"southern"
    # region term and pulls in an unrelated southern-states list
    "North Dakota, South Dakota and Montana Drought (Spring-Fall 2017)": ["MT", "ND", "SD"],
    # genuinely no usable geographic signal -> National
    "Severe Storms and Tornadoes (April 2002)": ["NATIONAL"],
    "Severe Storms/Hail (April 2003)": ["NATIONAL"],
    "Severe Storms/Tornadoes (May 2003)": ["NATIONAL"],
    "Severe Storms, Hail, Tornadoes (May 2004)": ["NATIONAL"],
    "Severe Storms and Tornadoes (March 2006)": ["NATIONAL"],
    "Numerous Wildfires (2006)": ["NATIONAL"],
    "Spring Freeze (April 2007)": ["AL", "AR", "GA", "IA", "IL", "IN", "KS", "KY", "MO", "MS", "NC", "NE", "OH", "OK", "SC", "TN", "VA", "WV"],
    "Arizona Severe Weather (October 2010)": ["AZ"],
    "Groundhog Day Blizzard (February 2011)": ["NATIONAL"],
}


def normalize_domain(d):
    if not isinstance(d, str):
        return None
    d = d.lower().strip()
    m = re.search(r"https?://([^/]+)", d)
    if m:
        d = m.group(1)
    d = d.rstrip("/*")
    d = re.sub(r"^\*\.", "", d)
    d = re.sub(r"^www\.", "", d)
    return d


# Domains that map to an extremely broad set of states (e.g. AP's per-state
# wire hub pages, apnews.com/hub/<state>) are wire-service aggregators, not
# state-specific local papers. Including them would make their PSE engine
# look "relevant" to nearly every state, defeating per-disaster engine
# subsetting. They're excluded from driving state -> engine inclusion, but
# remain in their engine's domain list (so they're still searched whenever
# that engine is selected via other, genuinely state-specific domains).
MAX_STATES_PER_DOMAIN = 4


def get_states_for_event(name):
    if name in HURRICANE_STATES:
        return sorted(set(HURRICANE_STATES[name]))

    core = name.split("(", 1)[0].strip().lower()
    # Normalize separators so multi-word region phrases (e.g. "north
    # central") are still matched when written as "north/central" or
    # "north-central".
    core = re.sub(r"[/\-]", " ", core)

    states = set()
    matched_terms = []
    for term, term_states in REGION_TO_STATES:
        # Word-boundary match, not plain substring: plain `term in core`
        # caused compound region words to spuriously match shorter terms
        # that are substrings of them (e.g. "midwest"/"southwest" contain
        # "west"; "southeast"/"northeast" contain "south"/"east" etc.),
        # producing massively over-broad unions for events with compound
        # region names like "Midwest/Southeast Tornadoes". Word boundaries
        # mean "west" only matches the standalone word "west", not the
        # "west" inside "midwest".
        m = re.search(r"\b" + re.escape(term) + r"\b", core)
        if m:
            matched_terms.append(term)
            states.update(term_states)
            # Blank out the matched text so its component words can't also
            # match a broader term later (e.g. once "north central" matches,
            # the standalone word "central" shouldn't separately trigger the
            # much broader "central" region; once "south carolina" matches,
            # "south" shouldn't separately trigger the broad "south" region).
            core = core[: m.start()] + " " * (m.end() - m.start()) + core[m.end() :]

    if not states:
        return ["NATIONAL"]

    if "NATIONAL" in states:
        return ["NATIONAL"]

    return sorted(states)


def main():
    # ---- Load PSE engine -> domain list, build domain -> [engines] ----
    pse = json.load(open(PSE_DOMAINS_PATH))
    domain_to_engines = {}
    for engine, info in pse.items():
        for d in info["domains"]:
            nd = normalize_domain(d)
            domain_to_engines.setdefault(nd, set()).add(engine)

    # ---- Build domain -> set(states), using the ROW-ALIGNED New URL /
    # Website URL columns (every row of Sheet1, not just the broken
    # Unique..HERE pipeline columns) ----
    df = pd.read_excel(AWN_PATH)
    domain_to_states = defaultdict(set)
    for _, row in df.iterrows():
        state = row["State"]
        if not isinstance(state, str) or not state.strip():
            continue
        for col in ("New URL", "Website URL"):
            nd = normalize_domain(row[col])
            if nd:
                domain_to_states[nd].add(state)

    # ---- Build state -> set of engines, via each PSE domain's real state(s) ----
    state_to_engines = {}
    state_domain_counts = {}
    skipped_broad = []
    skipped_unmatched = []
    for nd, engines in domain_to_engines.items():
        states = domain_to_states.get(nd, set())
        if not states:
            skipped_unmatched.append(nd)
            continue
        if len(states) > MAX_STATES_PER_DOMAIN:
            skipped_broad.append((nd, len(states)))
            continue
        for state in states:
            state_to_engines.setdefault(state, set()).update(engines)
            state_domain_counts[state] = state_domain_counts.get(state, 0) + 1

    state_engine_map = {
        state: sorted(engines) for state, engines in sorted(state_to_engines.items())
    }

    with open(OUT_STATE_ENGINE_MAP, "w") as f:
        json.dump(
            {
                "state_to_engines": state_engine_map,
                "state_domain_counts": state_domain_counts,
                "national_engine": "National",
                "excluded_broad_coverage_domains": skipped_broad,
                "unmatched_pse_domains": skipped_unmatched,
            },
            f,
            indent=2,
        )

    print("=== State -> covered engines (via row-aligned domain->state lookup) ===")
    for state, engines in state_engine_map.items():
        print(f"{state} ({state_domain_counts[state]} domains): {engines}")
    print(f"\nExcluded broad-coverage domains (> {MAX_STATES_PER_DOMAIN} states): {skipped_broad}")
    print(f"Unmatched PSE domains (no state found, kept in their engine but don't drive inclusion): {skipped_unmatched}")

    # ---- Assign states + engines to each event ----
    events = pd.read_csv(EVENTS_PATH)
    states_col = []
    engines_col = []
    national_only = 0
    for name in events["Name"]:
        states = get_states_for_event(name)
        states_col.append(",".join(states))

        engines = set()
        if "NATIONAL" in states:
            national_only += 1
            engines.add("National")
        else:
            for s in states:
                engines.update(state_engine_map.get(s, []))
            engines.add("National")

        engines_col.append(",".join(sorted(engines)))

    events["States"] = states_col
    events["Engines"] = engines_col
    events.to_csv(OUT_EVENTS, index=False)

    print(f"\nEvents with NATIONAL fallback (no specific states): {national_only} / {len(events)}")

    # engine count distribution
    counts = events["Engines"].apply(lambda x: len(x.split(",")))
    print("\nEngine-count-per-event distribution:")
    print(counts.value_counts().sort_index().to_string())
    print(f"\nMean engines/event: {counts.mean():.2f} (vs 19 currently)")


if __name__ == "__main__":
    main()
