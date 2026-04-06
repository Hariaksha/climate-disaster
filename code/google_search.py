import re
import time
import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

# API_KEY = os.environ["GOOGLE_API_KEY"] # find at Google Cloud Console --> APIs and Services --> Credentials. Then run "export GOOGLE_API_KEY=key" in terminal to make this code work
# API_KEY = "AIzaSyChGKqqjKV_xI6u-T58xbAf4GbYDNdxhD8"
API_KEY = "AIzaSyAUb_oRdWC6bC4E1jubjtzY9h5vlww_HMs"

CX_LIST = [
    "b36e8bb9027274d14",
    "510ce8c56df664249",
    "063fa5ba05c9e4960",
    "c24e943d956734fba",
    "9222ab0f29d42468e",
    "57468f00674154373",
    "738d6107764bf4044",
    "36b8dcfe1a83044fa",
    "f13d56b2a148e46a9",
    "8516f2fb95d884bc8",
    "503cd3fc501d241b1",
    "50bedac2eb0794a79",
    "730b35036c161454e",
    "27839fbbc50da4733",
    "02b59d2338bc94979",
    "94793f92aef4f4d16",
    "0521a26b5a0b74105",
    "930f9c358c52e4f78",
    "b637da095190d4ab4" # national
]

INPUT_FILE = "/Users/hariaksha/Documents/GitHub/climate-disaster/data/events-US-2000-2024-Q4.csv"
OUTPUT_FILE = "climate_disaster_article_urls.xlsx"

MAX_URLS_PER_DISASTER = 10
RESULTS_PER_ENGINE = 10
SLEEP_BETWEEN_CALLS = 0.25
GOOGLE_ENDPOINT = "https://customsearch.googleapis.com/customsearch/v1"


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

    queries = []

    if disaster_type == "Tropical Cyclone":
        queries.extend([
            f"\"{core_name}\" {year}",
            f"\"{core_name}\" {month_text} {year}",
            f"\"{core_name}\" storm {year}",
        ])

    elif disaster_type == "Wildfire":
        queries.extend([
            f"\"{core_name}\" wildfire {year}",
            f"\"{core_name}\" wildfires {year}",
            f"{region_text} wildfire {year}" if region_text else f"wildfire {year}",
            f"{region_text} fire {year}" if region_text else f"fire {year}",
        ])

    elif disaster_type == "Flooding":
        queries.extend([
            f"\"{core_name}\" {year}",
            f"{region_text} flooding {year}" if region_text else f"flooding {year}",
            f"{region_text} floods {month_text} {year}" if region_text else f"floods {month_text} {year}",
            f"{region_text} flash flood {year}" if "flash flood" in lower_name else ""
        ])

    elif disaster_type == "Drought":
        queries.extend([
            f"\"{core_name}\" {year}",
            f"{region_text} drought {year}" if region_text else f"drought {year}",
            f"{region_text} \"heat wave\" {year}" if region_text else f"\"heat wave\" {year}",
            f"drought \"heat wave\" {year}",
        ])

    elif disaster_type == "Winter Storm":
        queries.extend([
            f"\"{core_name}\" {year}",
            f"{region_text} \"winter storm\" {year}" if region_text else f"\"winter storm\" {year}",
            f"{region_text} \"cold wave\" {year}" if region_text else f"\"cold wave\" {year}",
            f"\"winter storm\" {month_text} {year}",
        ])

    elif disaster_type == "Freeze":
        queries.extend([
            f"\"{core_name}\" {year}",
            f"{region_text} freeze {year}" if region_text else f"freeze {year}",
            f"{region_text} frost freeze {year}" if region_text else f"frost freeze {year}",
            f"freeze {month_text} {year}",
        ])

    elif disaster_type == "Severe Storm":
        if "tornado" in lower_name and "hail" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year}",
                f"{region_text} tornado hail {year}" if region_text else f"tornado hail {year}",
                f"{region_text} \"severe weather\" {year}" if region_text else f"\"severe weather\" {year}",
                f"{region_text} tornado outbreak {year}" if region_text else f"tornado outbreak {year}",
            ])
        elif "tornado" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year}",
                f"{region_text} tornado outbreak {year}" if region_text else f"tornado outbreak {year}",
                f"{region_text} tornadoes {month_text} {year}" if region_text else f"tornadoes {month_text} {year}",
                f"{region_text} \"severe weather\" {year}" if region_text else f"\"severe weather\" {year}",
            ])
        elif "hail" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year}",
                f"{region_text} hail storm {year}" if region_text else f"hail storm {year}",
                f"{region_text} hail {month_text} {year}" if region_text else f"hail {month_text} {year}",
                f"{region_text} \"severe weather\" {year}" if region_text else f"\"severe weather\" {year}",
            ])
        elif "derecho" in lower_name:
            queries.extend([
                f"\"{core_name}\" {year}",
                f"{region_text} derecho {year}" if region_text else f"derecho {year}",
                f"{region_text} \"severe weather\" {year}" if region_text else f"\"severe weather\" {year}",
            ])
        else:
            queries.extend([
                f"\"{core_name}\" {year}",
                f"{region_text} \"severe weather\" {year}" if region_text else f"\"severe weather\" {year}",
                f"{region_text} storms {month_text} {year}" if region_text else f"storms {month_text} {year}",
                f"{region_text} tornadoes hail {year}" if region_text else f"tornadoes hail {year}",
            ])

    else:
        queries.extend([
            f"\"{core_name}\" {year}",
            f"\"{core_name}\" \"{disaster_type}\" {year}",
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

    link = item.get("link", "")
    dt = extract_date_from_url(link)
    return dt


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
        return True
    latest_ok = end_dt + timedelta(days=42)
    return begin_dt <= article_dt <= latest_ok


def search_one_engine(query, cx, begin_dt, end_dt, num=10):
    params = {
        "key": API_KEY,
        "cx": cx,
        "q": query,
        "num": min(num, 10),
        # "sort": f"date:r:{begin_dt.strftime('%Y%m%d')}:{(end_dt + timedelta(days=42)).strftime('%Y%m%d')}",
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
        if within_window(pub_dt, begin_dt, end_dt):
            cleaned.append({
                "url": link,
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "displayLink": item.get("displayLink", ""),
                "pub_date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "cx": cx,
            })

    return cleaned, None


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


def main():
    df = pd.read_csv(INPUT_FILE)

    required_cols = ["Name", "Disaster", "Begin Date", "End Date"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    all_output_rows = []
    total = len(df)

    # GLOBAL SUMMARY COUNTERS – define ONCE before the loop
    total_articles_saved = 0
    disasters_with_any_articles = 0
    disasters_with_zero_articles = 0

    # iterate newest → oldest
    for pos, idx in enumerate(reversed(df.index), start=1):
        row = df.loc[idx]
        name = str(row["Name"]).strip()
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

    out_df = pd.DataFrame(all_output_rows)
    out_df.to_excel(OUTPUT_FILE, index=False)

    print("\n=== FINAL SUMMARY ===")
    print(f"Total climate disasters processed: {total}")
    print(f"Climate disasters with at least 1 saved article: {disasters_with_any_articles}")
    print(f"Climate disasters with 0 saved articles: {disasters_with_zero_articles}")
    print(f"Total articles saved across all climate disasters: {total_articles_saved}")
    print(f"\nDone. Wrote {len(out_df)} rows to {OUTPUT_FILE}")
    

if __name__ == "__main__":
    main()