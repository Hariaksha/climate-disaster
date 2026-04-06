import re
import time
import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse


API_KEY = "AIzaSyAUb_oRdWC6bC4E1jubjtzY9h5vlww_HMs"


CX_LIST = [
    "b36e8bb9027274d14",
    "510ce8c56df664249",
    "063fa5ba05c9e4960",
    "c24e943d956734fba",
    "9222ab0f29d42468e",
    "57468f00674154373",
    "738d6107764bf4044",
]


INPUT_FILE = "/Users/hariaksha/Documents/GitHub/climate-disaster/data/events-US-2000-2024-Q4.csv"
OUTPUT_FILE = "climate_disaster_article_urls.csv"


MAX_URLS_PER_DISASTER = 50
RESULTS_PER_ENGINE = 10
MAX_URLS_PER_ENGINE = 3
SLEEP_BETWEEN_CALLS = 0.25
GOOGLE_ENDPOINT = "https://customsearch.googleapis.com/customsearch/v1"
GEO_SUFFIX = "United States"


# URL path segments that indicate non-article pages
BLOCKED_URL_PATTERNS = [
    "/sitemap", "/arc/outboundfeeds", "/tags/", "/tag/",
    "/category/", "/author/", "/feed/", "/rss/", "/page/",
    "/search/", "/weather/$", "weather/$",
]

# Domains known to return low-quality or off-topic results
BLOCKED_DOMAINS = {
    "dailyjournal.net",
    "arkansasonline.com",
}

US_TERMS = {
    "united states", "u.s.", " us ", "american", "noaa", "fema", "federal",
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
    "Drought":          {"drought", "heat wave", "dry", "water shortage", "arid", "drying"},
    "Tropical Cyclone": {"hurricane", "tropical storm", "cyclone", "storm surge", "landfall"},
    "Wildfire":         {"wildfire", "wild fire", "fire", "blaze", "evacuation", "acres burned", "brush fire"},
    "Flooding":         {"flood", "flooding", "flash flood", "inundation", "overflow", "deluge", "floodwater"},
    "Winter Storm":     {"winter storm", "blizzard", "snowstorm", "ice storm", "whiteout", "nor'easter", "snow"},
    "Freeze":           {"freeze", "frost", "freezing", "ice storm", "below zero", "cold snap", "frigid"},
    "Severe Storm":     {"tornado", "hail", "severe weather", "derecho", "thunderstorm", "outbreak", "twister", "funnel cloud"},
}

# Keywords that strongly indicate an off-topic article regardless of other matches
NOISE_PHRASES = [
    "cfp title", "college football", "nfl", "nba", "mlb", "nhl",
    "trump lobs", "election", "trump insults", "harris", "bootcamp",
    "internet access", "gas prices", "small businesses",
    "china flood", "china flooding", "africa drought", "kenya",
    "europe heat", "european heat", "mexico heat",
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
            f"\"{core_name}\" wildfires {year} {geo}",
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
        ])

    elif disaster_type == "Severe Storm":
        if "tornado" in lower_name and "hail" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} tornado hail {year} {geo}" if region_text else f"tornado hail {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
                f"{region_text} tornado outbreak {year} {geo}" if region_text else f"tornado outbreak {year} {geo}",
            ])
        elif "tornado" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} tornado outbreak {year} {geo}" if region_text else f"tornado outbreak {year} {geo}",
                f"{region_text} tornadoes {month_text} {year} {geo}" if region_text else f"tornadoes {month_text} {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
            ])
        elif "hail" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} hail storm {year} {geo}" if region_text else f"hail storm {year} {geo}",
                f"{region_text} hail {month_text} {year} {geo}" if region_text else f"hail {month_text} {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
            ])
        elif "derecho" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year} {geo}",
                f"{region_text} derecho {year} {geo}" if region_text else f"derecho {year} {geo}",
                f"{region_text} \"severe weather\" {year} {geo}" if region_text else f"\"severe weather\" {year} {geo}",
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

    return cleaned, begin_dt, end_dt


def extract_date_from_result(item):
    pagemap = item.get("pagemap", {})
    metatags = pagemap.get("metatags", [])
    candidates = []

    for mt in metatags:
        for k in [
            "article:published_time",
            "og:published_time",
            "pubdate",
            "publishdate",
            "date",
            "sailthru.date",
            "parsely-pub-date",
        ]:
            if mt.get(k):
                candidates.append(mt.get(k))

    for c in candidates:
        dt = try_parse_date(c)
        if dt:
            return dt

    return extract_date_from_url(item.get("link", ""))


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


def within_window(article_dt, begin_dt, end_dt):
    if article_dt is None:
        return False  # reject undated articles
    latest_ok = end_dt + timedelta(days=42)
    return begin_dt <= article_dt <= latest_ok


def is_blocked_url(url):
    """Reject non-article pages (sitemaps, tag pages, author pages, etc.)."""
    lower = url.lower()
    parsed = urlparse(url)
    if parsed.netloc.lower() in BLOCKED_DOMAINS:
        return True
    for pattern in BLOCKED_URL_PATTERNS:
        if pattern in lower:
            return True
    return False


def is_us_relevant(item):
    """Require at least 2 US geographic/institutional term matches."""
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    matches = sum(1 for term in US_TERMS if term in text)
    return matches >= 2


def is_disaster_relevant(item, disaster_type):
    """Require at least one disaster-type keyword in title + snippet."""
    keywords = DISASTER_KEYWORDS.get(disaster_type, set())
    if not keywords:
        return True
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    return any(kw in text for kw in keywords)


def is_noise_free(item):
    """Reject articles whose title/snippet contain known off-topic phrases."""
    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    return not any(phrase in text for phrase in NOISE_PHRASES)


def search_one_engine(query, cx, begin_dt, end_dt, disaster_type, num=10):
    params = {
        "key": API_KEY,
        "cx": cx,
        "q": query,
        "num": min(num, 10),
    }

    try:
        r = requests.get(GOOGLE_ENDPOINT, params=params, timeout=30)
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

        pub_dt = extract_date_from_result(item)
        if (not is_blocked_url(link)
                and within_window(pub_dt, begin_dt, end_dt)
                and is_us_relevant(item)
                and is_disaster_relevant(item, disaster_type)
                and is_noise_free(item)):
            cleaned.append({
                "url": link,
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "displayLink": item.get("displayLink", ""),
                "pub_date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
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
    return f"{scheme}://{netloc}{path}"


def save_results(all_output_rows, total, total_articles_saved,
                 disasters_with_any_articles, disasters_with_zero_articles,
                 interrupted=False):
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
    print(f"Total articles saved: {total_articles_saved}")
    print(f"Wrote {processed} rows to {OUTPUT_FILE}")


def main():
    df = pd.read_csv(INPUT_FILE)

    required_cols = ["Name", "Disaster", "Begin Date", "End Date"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    all_output_rows = []
    total = len(df)
    total_articles_saved = 0
    disasters_with_any_articles = 0
    disasters_with_zero_articles = 0
    interrupted = False

    try:
        for pos, idx in enumerate(reversed(df.index), start=1):
            row = df.loc[idx]
            name = str(row["Name"]).strip()
            disaster_type = str(row["Disaster"]).strip()
            print(f"\n=== Processing disaster {pos}/{total}: {name} ===")

            queries, begin_dt, end_dt = build_queries(row)
            candidates = []

            for query in queries:
                for cx in CX_LIST:
                    results, err = search_one_engine(
                        query=query,
                        cx=cx,
                        begin_dt=begin_dt,
                        end_dt=end_dt,
                        disaster_type=disaster_type,
                        num=RESULTS_PER_ENGINE,
                    )
                    if results:
                        candidates.extend(results)

                    time.sleep(SLEEP_BETWEEN_CALLS)

            candidates = dedupe_results(candidates)
            candidates = candidates[:MAX_URLS_PER_DISASTER]

            n_saved = len(candidates)
            total_articles_saved += n_saved

            if n_saved > 0:
                disasters_with_any_articles += 1
            else:
                disasters_with_zero_articles += 1

            print(f"Saved {n_saved} articles for disaster {pos}/{total}: {name}")

            out_row = row.to_dict()
            for i in range(1, MAX_URLS_PER_DISASTER + 1):
                out_row[f"URL {i}"] = candidates[i - 1]["url"] if i <= len(candidates) else ""

            all_output_rows.append(out_row)

    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user (Ctrl+C). Saving partial results…")
        interrupted = True

    finally:
        save_results(
            all_output_rows, total,
            total_articles_saved,
            disasters_with_any_articles,
            disasters_with_zero_articles,
            interrupted=interrupted,
        )


if __name__ == "__main__":
    main()