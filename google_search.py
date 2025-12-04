import pandas as pd
import requests, os, time

# CREDENTIALS AND FILES

API_KEY = "AIzaSyAUb_oRdWC6bC4E1jubjtzY9h5vlww_HMs" 
CSE_ID  = "a5a3e56ad70ea4717"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE  = os.path.join(SCRIPT_DIR, "events-US-1980-2024-Q4.csv")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "climate_disaster_articles.csv")

# Settings 
RESULTS_PER_EVENT = 5 # Number of articles per disaster to retrieve (max 10 per API call)
DATE_RESTRICT = None   # limit by recency, write None for no limit or "y5" for 5 year limit

# Function: Run a Google Custom Search API query
def google_search(query, num_results=10, date_restrict=None):
    url = "https://customsearch.googleapis.com/customsearch/v1"
    params = {
        "key": API_KEY,
        "cx": CSE_ID,
        "q": query,
        "num": min(num_results, 10),  # API max per request
    }
    if date_restrict:
        params["dateRestrict"] = date_restrict

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", [])

# Function: Build one search query from one row of the climate disaster CSV from NOAA
def build_query(row):
    parts = []

    # Event name (includes state/region and month/year text)
    if pd.notna(row.get("Name")):
        parts.append(str(row["Name"]))

    # Disaster type (e.g., Flooding, Drought, Tropical Cyclone)
    if pd.notna(row.get("Disaster")):
        parts.append(str(row["Disaster"]))

    # Extra keywords to steer toward relevant news
    parts.append("news")

    query = " ".join(parts)
    return query

def main():
    df = pd.read_csv(INPUT_FILE) # Load climate disaster dataset

    all_results = []

    print(f"Loaded {len(df)} disaster entries.\n Beginning scraping with Google Search API...\n")

    for idx, row in df.iterrows():
        if idx >= 2: # Limit to 2 events for testing
            break
        query = build_query(row)
        print(f"[{idx+1}/{len(df)}] Searching: {query}")

        try:
            items = google_search(
                query,
                num_results=RESULTS_PER_EVENT,
                date_restrict=DATE_RESTRICT
            )
        except Exception as e:
            print("  ERROR:", e)
            continue

        if not items:
            print("  No results returned.")
            continue

        # Use the row index as a simple event_id
        event_id = idx

        for rank, item in enumerate(items, start=1):
            all_results.append({
                "event_id": event_id,
                "name": row.get("Name"),
                "disaster_type": row.get("Disaster"),
                "begin_date": row.get("Begin Date"),
                "query": query,
                "result_rank": rank,
                "title": item.get("title"),
                "url": item.get("link"),
                "snippet": item.get("snippet"),
                "display_link": item.get("displayLink"),
            })

        time.sleep(0.5) # For API rate limits

    out_df = pd.DataFrame(all_results)
    out_df.to_csv(OUTPUT_FILE, index=False)

    print("\nDone.")
    print(f"Saved {len(out_df)} total articles to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
