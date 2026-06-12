import re
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urlparse


API_KEY = "AIzaSyAUb_oRdWC6bC4E1jubjtzY9h5vlww_HMs"


PSE_DOMAINS_PATH = "/Users/hariaksha/Documents/GitHub/climate-disaster/data/pse_domains.json"

# engine name ("National", "1".."18") -> CX id, loaded from pse_domains.json
with open(PSE_DOMAINS_PATH) as _f:
    ENGINE_CX = {engine: info["cx"] for engine, info in json.load(_f).items()}

# Full engine list (used as a fallback when a disaster's per-event engine
# subset returns 0 candidates).
ALL_CX_LIST = list(ENGINE_CX.values())


INPUT_FILE = "/Users/hariaksha/Documents/GitHub/climate-disaster/data/events-US-2000-2024-Q4-states.csv"
OUTPUT_FILE = "/Users/hariaksha/Documents/GitHub/climate-disaster/climate_disaster_article_urls.csv"
CHECKPOINT_FILE = "/Users/hariaksha/Documents/GitHub/climate-disaster/climate_disaster_checkpoint.csv"

PREFETCH_MULTIPLIER = 2   # ADD at top with other constants
MAX_URLS_PER_DISASTER = 30 # for production run
RESULTS_PER_ENGINE = 10
MAX_URLS_PER_ENGINE = 5 # used to be 3
SLEEP_BETWEEN_CALLS = 0.5
SLEEP_EVERY_N_CALLS = 50    # pause longer every N API calls
SLEEP_LONG = 10.0           # seconds for the longer pause
GOOGLE_ENDPOINT = "https://customsearch.googleapis.com/customsearch/v1"
GEO_SUFFIX = "United States"

TEST_MODE = True   # ← set to False for full production run
PRE2010_FOCUS_TEST = False  # ← set to False to use the normal TEST_MODE sample

# URL path segments that indicate non-article pages
BLOCKED_URL_PATTERNS = [
    "/sitemap", "/arc/outboundfeeds", "/tags/", "/tag/", "/photos-",
    "/category/", "/author/", "/feed/", "/rss/", "/page/",
    "/search/", "/weather/forecast", "/opinion/", "/letters/", "/obituaries/",
    "/weather/maps", "/weather/radar", "/weather/alerts", "/weather/conditions", "/elections/", "/results/race/", "/sports/",
    "/corrections/", "/newsletter/", "/newsletters/", "/podcast/", "/podcasts/", "/shows/", 
    "/channel/",  # YouTube channel page
    "/watch",              # YouTube watch URLs (also covered by youtube.com domain block below)
    "/wildfire-map",          # map pages, not articles
    "/storm-tracker", "/hurricane-tracker",     # tracker widgets
    "/drought-monitor",       # monitor/dashboard pages
    "/live-updates",          # live blog index pages (not individual articles)
    "/photo-gallery",         # gallery pages
    "/photos/", "/video/", "/live-blog",               # video pages without article text
    "/videos/", "/sponsored/",            # sponsored content
    "/advertorial/", "/press-release/",
]

# Domains known to return low-quality or off-topic results
BLOCKED_DOMAINS = {
    "dailyjournal.net",
    "arkansasonline.com",
    "youtube.com",
    "www.youtube.com",
    "youtu.be",
    "aspenjournalism.org", # newsletter-heavy, low article density
}

US_TERMS = {
    "united states", "u.s.", "noaa", "fema",
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "midwest", "southeast", "southwest",
    "northeast", "gulf coast", "great plains", "appalachian",
}

DISASTER_KEYWORDS = {
    "Drought":          {"drought", "heat wave", "water shortage", "dry spell", "record heat", "drought conditions", "water crisis", "extreme heat"},
    "Tropical Cyclone": {"hurricane", "tropical storm", "cyclone", "storm surge", "landfall"},
    "Wildfire":         {"wildfire", "wild fire", "wildfire season", "blaze", "evacuation order", "acres burned", "brush fire", "forest fire"},
    "Flooding":         {"flood", "flooding", "flash flood", "river flooding", "river overflow", "deluge", "floodwater", "flood warning"},
    "Winter Storm":     {"winter storm", "blizzard", "snowstorm", "ice storm", "whiteout", "nor'easter", "snow emergency", "heavy snow"},
    "Freeze":           {"freeze", "frost", "freezing", "ice storm", "below zero", "cold snap", "frigid"},
    "Severe Storm":     {"tornado", "hail", "severe weather", "derecho", "thunderstorm", "outbreak", "twister", "funnel cloud"},
}

# Keywords that strongly indicate an off-topic article regardless of other matches
NOISE_PHRASES = [
    "cfp title", "college football", "nfl", "nba", "mlb", "nhl", "trump lobs", "trump insults", "election results", "election day", "polling place", "voter turnout",
    "campaign trail", "bootcamp", "china flood", "china flooding", "africa drought", "western africa", "eastern africa", "north africa", "kenya", "europe heat", "european heat", "mexico heat", "pakistan fires",
    "pakistan military", "obama mccain", "debate transcript", "hedge fund", "mcclatchy", "newspaper chain", "mysterious object", "oil prices", "falling oil", "internet access", "gas prices",
    "bermuda", "canada water", "sex abuse lawsuit", "child abuse lawsuit"
]


def parse_yyyymmdd(x):
    s = str(int(x)) if isinstance(x, (int, float)) else str(x)
    s = s.strip()
    return datetime.strptime(s, "%Y%m%d")


def build_queries(row):
    raw_name = str(row["Name"]).strip()
    disaster_type = str(row["Disaster"]).strip()
    begin_dt = parse_yyyymmdd(row["Begin Date"])
    end_dt = parse_yyyymmdd(row["End Date"])
    year = begin_dt.year

    core_name = raw_name.split("(", 1)[0].strip()
    lower_name = core_name.lower()

    region_terms = [
        "southeast", "southern", "south", "eastern", "east", "western", "west",
        "northwestern", "northwest", "northeastern", "northeast", "midwest",
        "upper midwest", "ohio valley", "plains", "great plains", "rockies",
        "mid-atlantic", "north central", "south florida", "florida", "texas",
        "california", "new mexico", "kentucky", "missouri", "hawaii",
        "fort lauderdale", "east coast", "united states", "u.s."
    ]

    found_regions = []
    for r in region_terms:
        if r in lower_name:
            found_regions.append(r)

    found_regions = list(dict.fromkeys(found_regions))

    # Drop terms that are substrings of other matched terms (e.g. "south"
    # and "east" are redundant once "southeast" has matched; this avoids
    # garbled query text like "southeast south east").
    found_regions = [
        r for r in found_regions
        if not any(r != r2 and r in r2 for r2 in found_regions)
    ]

    region_text = " ".join(found_regions[:3]).strip()
    month_text = begin_dt.strftime("%B")
    geo = GEO_SUFFIX

    queries = []

    if disaster_type == "Tropical Cyclone":
        queries.extend([
            f"\"{core_name}\" {year} {geo}",
            f"\"{core_name}\" {month_text} {year} {geo}",
            f"\"{core_name}\" storm {year} {geo}",
        ])

    elif disaster_type == "Wildfire":
        queries.extend([
            f"\"{core_name}\" wildfire {year} {geo}",
            f"\"{core_name}\" fire evacuation {year} {geo}",   # ← adds new signal
            f"{region_text} wildfire {year} {geo}" if region_text else f"wildfire {year} {geo}",
            f"{region_text} fire {year} {geo}" if region_text else f"fire {year} {geo}",
        ])

    elif disaster_type == "Flooding":
        queries.extend([
            f"\"{core_name}\" {year} {geo}",
            f"{region_text} flooding {year} {geo}" if region_text else f"flooding {year} {geo}",
            f"{region_text} floods {month_text} {year} {geo}" if region_text else f"floods {month_text} {year} {geo}",
            f"{region_text} flash flood {year} {geo}" if "flash flood" in lower_name else ""
        ])

    elif disaster_type == "Drought":
        queries.extend([
            f"\"{core_name}\" {year} {geo}",
            f"{region_text} drought {year} {geo}" if region_text else f"drought {year} {geo}",
            f"{region_text} \"heat wave\" {year} {geo}" if region_text else f"\"heat wave\" {year} {geo}",
            f"drought \"heat wave\" {year} {geo}",
        ])

    elif disaster_type == "Winter Storm":
        queries.extend([
            f"\"{core_name}\" {year} {geo}",
            f"{region_text} \"winter storm\" {year} {geo}" if region_text else f"\"winter storm\" {year} {geo}",
            f"{region_text} \"cold wave\" {year} {geo}" if region_text else f"\"cold wave\" {year} {geo}",
            f"\"winter storm\" {month_text} {year} {geo}",
        ])

    elif disaster_type == "Freeze":
        queries.extend([
            f"\"{core_name}\" {year} {geo}",
            f"{region_text} freeze {year} {geo}" if region_text else f"freeze {year} {geo}",
            f"{region_text} frost freeze {year} {geo}" if region_text else f"frost freeze {year} {geo}",
            f"freeze {month_text} {year} {geo}",
            f"{region_text} \"hard freeze\" {year} {geo}" if region_text else f"\"hard freeze\" {year} {geo}",  
            f"{region_text} \"killing frost\" {year} {geo}" if region_text else f"\"killing frost\" {year} {geo}",
        ])

    elif disaster_type == "Severe Storm":
        if "tornado" in lower_name and "hail" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} tornado hail {year} {geo}" if region_text else f"tornado hail {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
                f"{region_text} tornado outbreak {year} {geo}" if region_text else f"tornado outbreak {year} {geo}",
                f"{region_text} \"storm damage\" {month_text} {year} {geo}" if region_text else f"\"storm damage\" {month_text} {year} {geo}",
            ])
        elif "tornado" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} tornado outbreak {year} {geo}" if region_text else f"tornado outbreak {year} {geo}",
                f"{region_text} tornadoes {month_text} {year} {geo}" if region_text else f"tornadoes {month_text} {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
                f"{region_text} \"storm damage\" {month_text} {year} {geo}" if region_text else f"\"storm damage\" {month_text} {year} {geo}",
            ])
        elif "hail" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} hail storm {year} {geo}" if region_text else f"hail storm {year} {geo}",
                f"{region_text} hail {month_text} {year} {geo}" if region_text else f"hail {month_text} {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
                f"{region_text} \"storm damage\" {month_text} {year} {geo}" if region_text else f"\"storm damage\" {month_text} {year} {geo}", 
            ])
        elif "derecho" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} derecho {year} {geo}" if region_text else f"derecho {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
                f"{region_text} \"storm damage\" {month_text} {year} {geo}" if region_text else f"\"storm damage\" {month_text} {year} {geo}",
            ])
        else:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
                f"{region_text} storms {month_text} {year} {geo}" if region_text else f"storms {month_text} {year} {geo}",
                f"{region_text} tornadoes hail {year} {geo}" if region_text else f"tornadoes hail {year} {geo}",
            ])

    else:
        queries.extend([
            f"\"{core_name}\" {year} {geo}",
            f"\"{core_name}\" \"{disaster_type}\" {year} {geo}",
        ])

    cleaned = []
    seen = set()
    for q in queries:
        q = q.strip()
        if not q:
            continue
        if q not in seen:
            seen.add(q)
            cleaned.append(q)

    return cleaned, begin_dt, end_dt, core_name, region_text


def extract_date_from_result(item):
    pagemap = item.get("pagemap", {})
    metatags = pagemap.get("metatags", [])
    candidates = []

    for mt in metatags:
        for k in [
            "article:published_time", "og:published_time",
            "pubdate", "publishdate", "date",
            "sailthru.date", "parsely-pub-date",
        ]:
            if mt.get(k):
                candidates.append(mt.get(k))

    for c in candidates:
        dt = try_parse_date(c)
        if dt:
            return dt, "metadata"   # ← return source

    url_dt = extract_date_from_url(item.get("link", ""))
    if url_dt:
        return url_dt, "url"        # ← return source

    return None, "none"             # ← return source


def try_parse_date(value):
    if not value:
        return None
    value = str(value).strip()

    patterns = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for p in patterns:
        try:
            dt = datetime.strptime(value, p)
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except:
            pass

    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", value)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except:
            return None

    return None


def extract_date_from_url(url):
    if not url:
        return None

    patterns = [
        r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/",
        r"[-_/](20\d{2})[-_/](0[1-9]|1[0-2])[-_/](0[1-9]|[12]\d|3[01])[-_/]",
    ]

    for p in patterns:
        m = re.search(p, url)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except:
                pass

    return None


def within_window(article_dt, begin_dt, end_dt, source="metadata", max_latest_dt=None):
    if article_dt is None:
        return False
    event_duration = (end_dt - begin_dt).days
    grace = max(21, min(42, event_duration * 3))  # 21 days, scaled by event length
    latest_ok = end_dt + timedelta(days=grace)
    # Don't let the grace period bleed into a later, separate event's coverage
    # (e.g. two distinct winter storms in the same month).
    if max_latest_dt is not None and max_latest_dt < latest_ok:
        latest_ok = max_latest_dt
    return begin_dt <= article_dt <= latest_ok


def is_blocked_url(url):
    """Reject non-article pages (sitemaps, tag pages, author pages, etc.)."""
    lower = url.lower()
    parsed = urlparse(url)

    if lower.endswith(".pdf"): # NEW: reject PDFs
        return True

    if parsed.path in ("", "/"): # NEW: reject bare homepages (path is empty or just "/")
        return True
    
    if parsed.netloc.lower() in BLOCKED_DOMAINS:
        return True
    for pattern in BLOCKED_URL_PATTERNS:
        if pattern in lower:
            return True
    return False


def is_us_relevant(item):
    """Require at least 1 US geographic/institutional term matches."""
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    matches = sum(1 for term in US_TERMS if term in text)
    return matches >= 1 # change from 2


def is_disaster_relevant(item, disaster_type):
    """Require disaster keyword in title, OR 2+ DISTINCT keyword mentions
    across title+snippet combined."""
    keywords = DISASTER_KEYWORDS.get(disaster_type, set())
    if not keywords:
        return True

    title = item.get("title", "").lower()
    snippet = item.get("snippet", "").lower()

    # Fast pass: keyword in title
    if any(kw in title for kw in keywords):
        return True

    # Fallback: needs 2+ keyword hits across title+snippet combined.
    # Match longest keywords first and scan non-overlapping so that e.g. a
    # single mention of "flooding" doesn't also count as "flood" (which is
    # a substring of "flooding") and inflate the hit count to 2.
    combined = title + " " + snippet
    sorted_kw = sorted(keywords, key=len, reverse=True)
    pattern = re.compile("|".join(re.escape(k) for k in sorted_kw))
    hits = len(pattern.findall(combined))
    return hits >= 2


# A compact mapping from terms that appear in disaster names → geographic keywords
# to require in title/snippet
GEO_KEYWORDS = {
    # Cities
    "houston":          {"houston", "harris county", "texas"},
    "fort lauderdale":  {"fort lauderdale", "broward", "florida"},

    # States mentioned by name in event titles
    "california":       {"california"},
    "texas":            {"texas"},
    "florida":          {"florida", "south florida"},
    "louisiana":        {"louisiana"},
    "georgia":          {"georgia"},
    "illinois":         {"illinois"},
    "colorado":         {"colorado"},
    "kentucky":         {"kentucky"},
    "missouri":         {"missouri"},
    "minnesota":        {"minnesota"},
    "mississippi":      {"mississippi"},
    "tennessee":        {"tennessee"},
    "arkansas":         {"arkansas"},
    "oklahoma":         {"oklahoma"},
    "arizona":          {"arizona"},
    "hawaii":           {"hawaii", "maui", "lahaina"},
    "west virginia":    {"west virginia"},
    "new mexico":       {"new mexico"},
    "south carolina":   {"south carolina"},
    "north dakota":     {"north dakota"},
    "south dakota":     {"south dakota"},
    "montana":          {"montana"},

    "ohio":             {"ohio"},
    "indiana":          {"indiana"},
    "iowa":             {"iowa"},
    "nebraska":         {"nebraska"},
    "kansas":           {"kansas"},
    "virginia":         {"virginia"},  # careful — also matches "west virginia"
    "oregon":           {"oregon"},
    "washington":       {"washington", "pacific northwest"},
    "idaho":            {"idaho"},
    "nevada":           {"nevada"},
    "utah":             {"utah"},
    "wyoming":          {"wyoming"},
    "north carolina":   {"north carolina"},
    "new york":         {"new york"},
    "new jersey":       {"new jersey"},
    "michigan":         {"michigan"},
}

# Broad multi-state regions that appear in event names. Unlike GEO_KEYWORDS
# (specific single places, checked individually with AND logic), these are
# checked with OR logic across the union of all matched regions' state
# lists — an event can span multiple overlapping regions (e.g.
# "Southwest/Southern Plains Drought"), and an article only needs to
# reference ONE state/term from that combined area to be considered
# geographically relevant.
REGION_GEO_TERMS = {
    "southwest":        {"arizona", "new mexico", "texas", "nevada", "utah", "colorado", "southwest"},
    "southern plains":  {"texas", "oklahoma", "kansas", "new mexico", "southern plains"},
    "great plains":     {"kansas", "nebraska", "oklahoma", "iowa", "north dakota", "south dakota", "montana", "colorado", "texas", "great plains"},
    "southeast":        {"alabama", "florida", "georgia", "kentucky", "mississippi", "north carolina",
                          "south carolina", "tennessee", "virginia", "west virginia", "southeast"},
    "northeast":        {"maine", "new hampshire", "vermont", "massachusetts", "rhode island", "connecticut",
                          "new york", "new jersey", "pennsylvania", "northeast"},
    "pacific northwest":{"washington", "oregon", "idaho", "montana", "pacific northwest", "northwest"},
    "northwest":        {"washington", "oregon", "idaho", "montana", "northwest"},
    "midwest":          {"illinois", "indiana", "iowa", "kansas", "michigan", "minnesota", "missouri",
                          "nebraska", "north dakota", "ohio", "south dakota", "wisconsin", "midwest"},
    "mid-atlantic":     {"new york", "new jersey", "pennsylvania", "delaware", "maryland", "virginia",
                          "west virginia", "district of columbia", "mid-atlantic"},
    "rockies":          {"colorado", "wyoming", "montana", "idaho", "utah", "rockies", "rocky mountains"},
    "gulf coast":       {"texas", "louisiana", "mississippi", "alabama", "florida", "gulf coast"},
    "ohio valley":      {"ohio", "kentucky", "indiana", "west virginia", "illinois", "tennessee", "ohio valley"},
    "appalachian":      {"appalachian", "west virginia", "kentucky", "virginia"},

    # "...ern" adjective forms (e.g. "Northeastern Winter Storm"). \b-bounded
    # regex matching on "northeast" etc. does NOT match "northeastern" (no
    # word boundary between "northeast" and "ern"), so without these explicit
    # entries such event names get NO region match at all and skip the geo
    # check entirely — letting in articles about unrelated regions (e.g. an
    # Oregon/Washington article slipping into a "Northeastern Winter Storm"
    # result set).
    "northeastern":     {"maine", "new hampshire", "vermont", "massachusetts", "rhode island", "connecticut",
                          "new york", "new jersey", "pennsylvania", "northeast", "northeastern"},
    "southeastern":      {"alabama", "florida", "georgia", "kentucky", "mississippi", "north carolina",
                          "south carolina", "tennessee", "virginia", "west virginia", "southeast", "southeastern"},
    "northwestern":     {"washington", "oregon", "idaho", "montana", "northwest", "northwestern"},
    "southwestern":     {"arizona", "new mexico", "texas", "nevada", "utah", "colorado", "southwest", "southwestern"},
}


def is_geo_relevant(item, core_name, n_states=None):
    """If the disaster name contains a specific place, require it in title/snippet.

    `n_states` is the number of states the event actually spans (from the
    "States" column), or None for NATIONAL events.
    """
    lower_name = core_name.lower()
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()

    # Broad regions: if any matched, the article must mention at least one
    # state/term from the UNION of all matched regions' state lists (OR).
    # Checked first because when an event name combines a broad region with
    # a more specific place fragment (e.g. "Southeast, Ohio Valley and
    # Northeast Severe Weather" — "ohio" alone is a GEO_KEYWORDS entry too),
    # the broad-region check with its ratio safeguard is the more reliable
    # signal and should take precedence over the narrower GEO_KEYWORDS match.
    region_union = set()
    region_matched = False
    for region, required_terms in REGION_GEO_TERMS.items():
        pattern = r'\b' + re.escape(region) + r'\b'
        if re.search(pattern, lower_name):
            region_matched = True
            region_union.update(required_terms)

    if region_matched:
        # If the matched region(s) cover only a small slice of the event's
        # actual footprint, the union is too narrow to be a reliable
        # geographic filter (e.g. "Northwest, Central, Eastern Winter Storm"
        # spans 44 states, but "northwest" alone only maps to ~5 — most
        # legitimate coverage of the storm wouldn't mention those). Skip the
        # check entirely in that case rather than risk rejecting valid
        # articles.
        if n_states and n_states > 0 and (len(region_union) / n_states) < 0.3:
            return True
        return any(t in text for t in region_union)

    # Specific places (cities/states): only checked if no broad region
    # matched above. The article must mention at least one term from the
    # UNION of all matched places' required terms (OR). When an event name
    # lists multiple states/places (e.g. "North Dakota, South Dakota and
    # Montana Drought"), articles typically cover only one of them, so
    # OR-across-matches is correct — AND would require every listed place to
    # appear in a single article, which almost never happens.
    geo_union = set()
    geo_matched = False
    for place, required_terms in GEO_KEYWORDS.items():
        pattern = r'\b' + re.escape(place) + r'\b'
        if re.search(pattern, lower_name):
            geo_matched = True
            geo_union.update(required_terms)

    if geo_matched:
        return any(t in text for t in geo_union)

    return True


# Pattern to extract a named tropical cyclone's storm name from an event's
# core name, e.g. "Hurricane Nicholas" -> "Nicholas", "Tropical Storm
# Allison" -> "Allison".
STORM_NAME_PATTERN = re.compile(
    r"(?:Hurricane|Tropical Storm|Tropical Depression|Typhoon|Tropical Cyclone)\s+([A-Za-z]+)",
    re.IGNORECASE,
)


def is_storm_relevant(item, core_name, disaster_type):
    """For named tropical cyclones, require the storm's own name to appear
    in the article — prevents e.g. a "Tropical Storm Sam" article from
    being matched to "Hurricane Nicholas" just because both are generic
    hurricane coverage."""
    if disaster_type != "Tropical Cyclone":
        return True

    m = STORM_NAME_PATTERN.search(core_name)
    if not m:
        return True

    storm_name = m.group(1).lower()
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    return re.search(r'\b' + re.escape(storm_name) + r'\b', text) is not None

def is_noise_free(item):
    """Reject articles whose title/snippet contain known off-topic phrases."""
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    return not any(phrase in text for phrase in NOISE_PHRASES)


def search_one_engine(query, cx, begin_dt, end_dt, disaster_type, core_name, num=10, n_states=None, next_event_begin_dt=None):
    event_duration = (end_dt - begin_dt).days
    if disaster_type in ("Winter Storm", "Severe Storm"):
        # These are short, fast-moving events, and successive storms/outbreaks
        # often hit the same region within 2-3 weeks. A full 21-42 day grace
        # period risks pulling in coverage of the *next* storm rather than
        # follow-up coverage of this one, so use a shorter cap.
        grace = max(10, min(21, event_duration * 3))
    else:
        grace = max(21, min(42, event_duration * 3))
    latest_ok = end_dt + timedelta(days=grace)
    # Cap the grace period so it doesn't extend into a later, separate event
    # of the same disaster type (avoids pulling in that event's coverage).
    if next_event_begin_dt is not None:
        cap = next_event_begin_dt - timedelta(days=1)
        if cap < latest_ok:
            latest_ok = max(cap, end_dt)  # never shrink below the event's own end date
    params = {"key": API_KEY, "cx": cx, "q": query, "num": min(num, 10)}

    params["sort"] = f"date:r:{begin_dt.strftime('%Y%m%d')}:{latest_ok.strftime('%Y%m%d')}"

    try:
        r = requests.get(GOOGLE_ENDPOINT, params=params, timeout=30)
        if r.status_code == 429:
            return [], "RATE_LIMITED"      # ← distinct signal
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return [], str(e)

    items = data.get("items", [])
    cleaned = []

    for item in items:
        link = item.get("link", "").strip()
        if not link:
            continue

        pub_dt, date_source = extract_date_from_result(item)   # ← unpack

        if (not is_blocked_url(link)
                and within_window(pub_dt, begin_dt, end_dt, source=date_source, max_latest_dt=latest_ok)  # ← pass source
                and is_us_relevant(item)
                and is_disaster_relevant(item, disaster_type)
                and is_geo_relevant(item, core_name, n_states=n_states)
                and is_storm_relevant(item, core_name, disaster_type)
                and is_noise_free(item)):
            cleaned.append({
                "url": link,
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "displayLink": item.get("displayLink", ""),
                "pub_date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "date_source": date_source,   # ← log this for debugging
                "cx": cx,
            })


    return cleaned[:MAX_URLS_PER_ENGINE], None


def dedupe_results(results):
    seen = set()
    out = []

    for r in results:
        url = r["url"]
        norm = normalize_url(url)
        if norm not in seen:
            seen.add(norm)
            out.append(r)

    return out


def normalize_url(url):
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    netloc = netloc.removeprefix("www.")
    return f"{scheme}://{netloc}{path}"


def save_results(all_output_rows, total, total_articles_saved,
                 disasters_with_any_articles, disasters_with_zero_articles,
                 type_counts=None, interrupted=False):
    if not all_output_rows:
        print("\nNo data collected — output file not written.")
        return

    out_df = pd.DataFrame(all_output_rows)
    out_df.to_csv(OUTPUT_FILE, index=False)

    label = "INTERRUPTED – PARTIAL" if interrupted else "FINAL"
    processed = len(all_output_rows)

    print(f"\n=== {label} SUMMARY ===")
    print(f"Climate disasters processed: {processed} / {total}")
    print(f"  with ≥1 article : {disasters_with_any_articles}")
    print(f"  with 0 articles : {disasters_with_zero_articles}")
    if type_counts:
        print("\n  Articles by disaster type:")
        for dtype, count in sorted(type_counts.items()):
            print(f"    {dtype:<20}: {count}")
    print(f"Total articles saved: {total_articles_saved}")
    print(f"Wrote {processed} rows to {OUTPUT_FILE}")


def main():
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded: {len(df)} rows | Columns: {list(df.columns)}")

    required_cols = ["Name", "Disaster", "Begin Date", "End Date"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # For each event, find the begin date of the next event of the *same*
    # disaster type (computed against the full dataset, before any TEST_MODE
    # sampling, so the cap is correct even when only a subset is processed).
    # Used to keep a short event's post-event "grace period" from bleeding
    # into a later, separate event's coverage (e.g. two distinct winter
    # storms in the same month).
    next_event_begin_by_name = {}
    for dtype, grp in df.groupby("Disaster"):
        begins = sorted(parse_yyyymmdd(b) for b in grp["Begin Date"])
        for _, r in grp.iterrows():
            end_dt = parse_yyyymmdd(r["End Date"])
            later = [b for b in begins if b > end_dt]
            next_event_begin_by_name[str(r["Name"]).strip()] = min(later) if later else None


    # ── TEST RUN: sample 15 random disasters ──────────────────────────
    # df = df.sample(n=10, random_state=12).reset_index(drop=True)
    # ──────────────────────────────────────────────────────────────────

    if TEST_MODE and PRE2010_FOCUS_TEST:
        # Focused test on pre-2010 events to check the impact of the
        # within_window / sort=date: fixes for older disasters (which often
        # lack structured published-time metadata and rely on URL-derived dates).
        years = df["Begin Date"].astype(str).str[:4].astype(int)
        pre2010_df = df[years < 2010]

        # 2 random pre-2010 events per disaster type (bounded by availability)
        sample_df = pd.concat([
            grp.sample(min(len(grp), 2), random_state=44)
            for _, grp in pre2010_df.groupby("Disaster", group_keys=False)
        ])

        df = sample_df.reset_index(drop=True)
        print(f"[TEST MODE - PRE-2010 FOCUS] {len(df)} pre-2010 disasters "
              f"({len(pre2010_df)} pre-2010 events available)")
    elif TEST_MODE:
        # Pin known problem cases + a sample of well-covered ones for comparison.
        # This batch targets the events touched by this session's fixes:
        # the Spring Freeze 2007 reclassification, the 5 rows that gained
        # missing states (OK/KS/MI/AK/MT/ND/SD), and the Feb 2023 winter
        # storm grace-period fix.
        problem_names = [
            "Spring Freeze (April 2007)",
            "Oklahoma, Kansas, and Texas Tornadoes and Severe Weather (May 2010)",
            "Michigan and Northeast Flooding (August 2014)",
            "Texas and Oklahoma Flooding and Severe Weather (May 2015)",
            "North Dakota, South Dakota and Montana Drought (Spring-Fall 2017)",
            "California and Alaska Wildfires (Summer-Fall 2019)",
            "Texas and Oklahoma Severe Weather (April 2021)",
            "Northeastern Winter Storm/Cold Wave (February 2023)",
        ]
        problem_mask = df["Name"].apply(lambda n: any(p in n for p in problem_names))
        problem_df = df[problem_mask]

        # Fill remaining slots with 2 random per type for baseline comparison
        sample_df = pd.concat([
            grp.sample(min(len(grp), 2), random_state=44)
            for _, grp in df[~problem_mask].groupby("Disaster", group_keys=False)
        ])

        df = pd.concat([problem_df, sample_df]).reset_index(drop=True)
        print(f"[TEST MODE] {len(df)} disasters ({len(problem_df)} problem cases + {len(sample_df)} baseline)")
    else:
        print(f"[PRODUCTION] Running full {len(df)} disasters")

    all_output_rows = []
    total = len(df)
    total_articles_saved = 0
    disasters_with_any_articles = 0
    disasters_with_zero_articles = 0
    interrupted = False

    call_count = 0  # initialize before the outer for loop
    type_counts = {} 
    
    try:
        for pos, idx in enumerate(df.index, start=1):
            row = df.loc[idx]
            name = str(row["Name"]).strip()
            disaster_type = str(row["Disaster"]).strip()
            print(f"\n=== Processing disaster {pos}/{total}: {name} ===")

            queries, begin_dt, end_dt, core_name, region_text = build_queries(row)
            candidates = []

            next_event_begin_dt = next_event_begin_by_name.get(name)

            # Number of states this event actually spans (None for NATIONAL),
            # used by is_geo_relevant to decide whether a matched region's
            # required-terms union is representative enough to enforce.
            states_field = str(row.get("States", "")).strip().upper()
            if not states_field or states_field == "NATIONAL" or states_field == "NAN":
                n_states = None
            else:
                n_states = len([s for s in states_field.split(",") if s.strip()])

            # Per-disaster engine subset (from the "Engines" column produced by
            # build_state_engine_mapping.py). Falls back to all engines if the
            # column is missing/empty.
            engines_field = str(row.get("Engines", "")).strip()
            if engines_field and engines_field.lower() != "nan":
                engine_names = [e.strip() for e in engines_field.split(",") if e.strip()]
                cx_list = [ENGINE_CX[e] for e in engine_names if e in ENGINE_CX]
            else:
                cx_list = []
            if not cx_list:
                cx_list = ALL_CX_LIST

            print(f"  [Engines] Querying {len(cx_list)}/{len(ALL_CX_LIST)} engines")

            for query in queries:
                if len(candidates) >= MAX_URLS_PER_DISASTER * PREFETCH_MULTIPLIER:
                    break                          # ← stop querying early

                for cx in cx_list:
                    call_count += 1
                    if len(candidates) >= MAX_URLS_PER_DISASTER * PREFETCH_MULTIPLIER: 
                        break                      # ← stop across engines too
                    results, err = search_one_engine(
                        query=query,
                        cx=cx,
                        begin_dt=begin_dt,
                        end_dt=end_dt,
                        disaster_type=disaster_type,
                        core_name=core_name,          # ← add this
                        num=RESULTS_PER_ENGINE,
                        n_states=n_states,
                        next_event_begin_dt=next_event_begin_dt,
                    )
                    if err == "RATE_LIMITED":
                        print(f"  [429] Rate limited — sleeping 60s before retrying…")
                        time.sleep(60)
                        results, err = search_one_engine(query=query, cx=cx, begin_dt=begin_dt, end_dt=end_dt, disaster_type=disaster_type, core_name=core_name, num=RESULTS_PER_ENGINE, n_states=n_states, next_event_begin_dt=next_event_begin_dt)
                    if err:
                        print(f"  [API error] cx={cx[:8]}… query='{query[:40]}': {err}")
                    elif results:
                        candidates.extend(results)

                    if call_count % SLEEP_EVERY_N_CALLS == 0:
                        print(f"  [Rate limit pause] {call_count} calls made, sleeping {SLEEP_LONG}s…")
                        time.sleep(SLEEP_LONG)
                    else:
                        time.sleep(SLEEP_BETWEEN_CALLS)

            # ADD after the query loop, before dedupe_results:
            if not candidates:
                fallback_queries = [
                    f"{disaster_type.lower()} {region_text} {begin_dt.year} {GEO_SUFFIX}" if region_text 
                    else f"{disaster_type.lower()} {begin_dt.year} {GEO_SUFFIX}",
                    f"{disaster_type.lower()} damage {begin_dt.strftime('%B')} {begin_dt.year} {GEO_SUFFIX}",
                ]
                print(f"  [Fallback] 0 candidates — trying {len(fallback_queries)} broader queries across all {len(ALL_CX_LIST)} engines…")
                for fq in fallback_queries:
                    for cx in ALL_CX_LIST:
                        results, err = search_one_engine(
                            query=fq, cx=cx, begin_dt=begin_dt, end_dt=end_dt,
                            disaster_type=disaster_type, core_name=core_name, num=RESULTS_PER_ENGINE,
                            n_states=n_states,
                            next_event_begin_dt=next_event_begin_dt,
                        )
                        if results:
                            candidates.extend(results)
                        call_count += 1
                        time.sleep(SLEEP_BETWEEN_CALLS)
                    if candidates:
                        break   # stop fallback as soon as we get anything

            candidates = dedupe_results(candidates)
            candidates = candidates[:MAX_URLS_PER_DISASTER]

            out_row = row.to_dict()
            for i in range(1, MAX_URLS_PER_DISASTER + 1):
                if i <= len(candidates):
                    out_row[f"URL {i}"] = candidates[i - 1]["url"]
                    out_row[f"Title {i}"] = candidates[i - 1]["title"]
                    out_row[f"Snippet {i}"] = candidates[i - 1]["snippet"]
                else:
                    out_row[f"URL {i}"] = ""
                    out_row[f"Title {i}"] = ""
                    out_row[f"Snippet {i}"] = ""


            all_output_rows.append(out_row)

            n_saved = len(candidates)
            type_counts[disaster_type] = type_counts.get(disaster_type, 0) + n_saved
            total_articles_saved += n_saved

            if n_saved > 0:
                disasters_with_any_articles += 1
            else:
                disasters_with_zero_articles += 1

            print(f"Saved {n_saved} articles for disaster {pos}/{total}: {name}")

            if pos % 25 == 0:
                pd.DataFrame(all_output_rows).to_csv(CHECKPOINT_FILE, index=False)
                print(f"[Checkpoint] Saved {pos} rows to {CHECKPOINT_FILE}")

    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user (Ctrl+C). Saving partial results…")
        interrupted = True

    finally:
        save_results(
            all_output_rows, total,
            total_articles_saved,
            disasters_with_any_articles,
            disasters_with_zero_articles,
            type_counts=type_counts,
            interrupted=interrupted,
        )


if __name__ == "__main__":
    main()