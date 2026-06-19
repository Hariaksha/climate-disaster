#!/usr/bin/env python3
"""
Phase 1: Submit all Master sheet URLs to archive.ph and record archive URLs.

The script is fully resumable — if it crashes or you stop it, rerun and it will
skip rows already marked 'found' or 'submitted' and pick up where it left off.

The script auto-detects rate limiting and pauses itself — no need to monitor it.

Results are written to two new columns in the Master sheet:
  Col 20: Archive URL    — the archive.ph URL for this article
  Col 21: Archive Status — 'found', 'submitted', or 'failed'
"""

import time
import re
import requests
import openpyxl
from bs4 import BeautifulSoup
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
WORKBOOK_PATH = '/Users/hariaksha/Documents/GitHub/climate-disaster/attribution.xlsx'
SHEET_NAME    = 'Master'
URL_COL       = 12   # Column L: original URL
ARCHIVE_URL_COL    = 20  # Column T: archive.ph URL (new)
ARCHIVE_STATUS_COL = 21  # Column U: status (new)

DELAY_EXISTING  = 4    # seconds between requests when archive already exists
DELAY_SUBMIT    = 35   # seconds to wait after submitting a new archive request
MAX_RETRIES     = 2    # retries per URL before marking as failed

# Rate-limit auto-pause settings
CONSEC_FAIL_THRESHOLD = 5    # pause after this many consecutive failures
# Backoff schedule (seconds): pause gets longer each time we hit the threshold
BACKOFF_SCHEDULE = [5*60, 10*60, 20*60, 30*60]  # 5, 10, 20, 30 minutes
BACKOFF_COUNTDOWN_INTERVAL = 60  # print countdown every N seconds

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
}
# ─────────────────────────────────────────────────────────────────────────────

# Global rate-limit state
consecutive_failures = 0
backoff_level = 0  # index into BACKOFF_SCHEDULE


def countdown_pause(seconds: int, reason: str):
    """Pause for `seconds`, printing a countdown so you can see what's happening."""
    print(f'\n⏸  {reason}')
    print(f'   Pausing for {seconds // 60}m {seconds % 60}s — script will resume automatically.')
    end_time = time.time() + seconds
    while True:
        remaining = int(end_time - time.time())
        if remaining <= 0:
            break
        mins, secs = divmod(remaining, 60)
        print(f'   Resuming in {mins}m {secs:02d}s ...', end='\r', flush=True)
        time.sleep(min(BACKOFF_COUNTDOWN_INTERVAL, remaining))
    print(f'\n▶  Resuming now.\n')


def is_valid_archive_url(url: str) -> bool:
    """Check that a URL looks like a real archive.ph snapshot, not a search/submit page."""
    if not url:
        return False
    if 'archive.ph' not in url and 'archive.today' not in url:
        return False
    # Valid snapshots look like: https://archive.ph/AbC12  (path is alphanumeric, 4-6 chars)
    # Invalid: https://archive.ph/submit, https://archive.ph/search, https://archive.ph/newest/...
    bad_paths = ['/submit', '/search', '/newest/', '/#', '/about', '/faq']
    for bad in bad_paths:
        if bad in url:
            return False
    return True


def is_rate_limited(response) -> bool:
    """Return True if the response looks like a rate-limit or Cloudflare block."""
    if response is None:
        return False
    if response.status_code in (429, 503):
        return True
    # Cloudflare challenge pages contain specific text
    body_lower = response.text[:2000].lower()
    if 'too many requests' in body_lower or 'rate limit' in body_lower:
        return True
    if 'cf-ray' in response.headers and response.status_code == 403:
        return True
    return False


def check_existing(url: str, session: requests.Session):
    """
    Check if archive.ph already has this URL.
    Returns (archive_url_or_None, rate_limited_bool).
    """
    try:
        check_url = f'https://archive.ph/newest/{url}'
        r = session.get(check_url, headers=HEADERS, timeout=25, allow_redirects=True)
        if is_rate_limited(r):
            return None, True
        final = r.url
        if r.status_code == 200 and is_valid_archive_url(final):
            return final, False
        soup = BeautifulSoup(r.text, 'lxml')
        canonical = soup.find('link', rel='canonical')
        if canonical and is_valid_archive_url(canonical.get('href', '')):
            return canonical['href'], False
    except Exception:
        pass
    return None, False


def submit_new(url: str, session: requests.Session):
    """
    Submit URL to archive.ph for archiving.
    Returns (archive_url_or_None, rate_limited_bool).
    """
    try:
        r = session.post(
            'https://archive.ph/submit/',
            data={'url': url, 'anyway': '1'},
            headers=HEADERS,
            timeout=60,
            allow_redirects=True,
        )
        if is_rate_limited(r):
            return None, True
        final = r.url
        if is_valid_archive_url(final):
            return final, False
        refresh = r.headers.get('Refresh', '')
        m = re.search(r'url=(https://archive\.ph/\w+)', refresh, re.IGNORECASE)
        if m and is_valid_archive_url(m.group(1)):
            return m.group(1), False
        soup = BeautifulSoup(r.text, 'lxml')
        canonical = soup.find('link', rel='canonical')
        if canonical and is_valid_archive_url(canonical.get('href', '')):
            return canonical['href'], False
    except Exception:
        pass
    return None, False


def handle_rate_limit():
    """Trigger an automatic backoff pause when rate limiting is detected."""
    global backoff_level, consecutive_failures
    pause_secs = BACKOFF_SCHEDULE[min(backoff_level, len(BACKOFF_SCHEDULE) - 1)]
    backoff_level = min(backoff_level + 1, len(BACKOFF_SCHEDULE) - 1)
    consecutive_failures = 0
    countdown_pause(pause_secs, f'Rate limit detected — auto-pausing for {pause_secs // 60} minutes.')


def get_archive_url(url: str, session: requests.Session) -> tuple:
    """
    Try to get an archive.ph URL for the given URL.
    Returns (archive_url, status) where status is 'found', 'submitted', or 'failed'.
    Automatically handles rate limiting with backoff pauses.
    """
    global consecutive_failures, backoff_level

    # Step 1: Check if already archived
    archive_url, rate_limited = check_existing(url, session)
    if rate_limited:
        handle_rate_limit()
        archive_url, rate_limited = check_existing(url, session)  # retry after pause
    if archive_url:
        consecutive_failures = 0
        backoff_level = max(0, backoff_level - 1)  # ease off backoff on success
        return archive_url, 'found'

    # Step 2: Submit for archiving
    print(f'      → Not found, submitting to archive.ph...')
    archive_url, rate_limited = submit_new(url, session)
    if rate_limited:
        handle_rate_limit()
        archive_url, rate_limited = submit_new(url, session)  # retry after pause
    if archive_url:
        consecutive_failures = 0
        backoff_level = max(0, backoff_level - 1)
        return archive_url, 'submitted'

    # Step 3: Wait and check again (archiving sometimes takes a moment)
    print(f'      → Waiting {DELAY_SUBMIT}s for archive to be ready...')
    time.sleep(DELAY_SUBMIT)
    archive_url, _ = check_existing(url, session)
    if archive_url:
        consecutive_failures = 0
        return archive_url, 'submitted'

    return None, 'failed'


def add_headers_if_missing(ws):
    """Add Archive URL and Archive Status column headers if not already present."""
    if ws.cell(row=1, column=ARCHIVE_URL_COL).value != 'Archive URL':
        ws.cell(row=1, column=ARCHIVE_URL_COL).value = 'Archive URL'
    if ws.cell(row=1, column=ARCHIVE_STATUS_COL).value != 'Archive Status':
        ws.cell(row=1, column=ARCHIVE_STATUS_COL).value = 'Archive Status'


def main():
    print(f'Loading workbook: {WORKBOOK_PATH}')
    wb = openpyxl.load_workbook(WORKBOOK_PATH)
    ws = wb[SHEET_NAME]
    add_headers_if_missing(ws)

    total_rows = ws.max_row - 1  # subtract header
    session = requests.Session()

    global consecutive_failures, backoff_level
    found = 0
    submitted = 0
    failed = 0
    skipped = 0

    start_time = datetime.now()
    print(f'Starting at {start_time.strftime("%H:%M:%S")} — {total_rows} articles to process\n')

    for row_idx in range(2, ws.max_row + 1):
        url = ws.cell(row=row_idx, column=URL_COL).value
        existing_status = ws.cell(row=row_idx, column=ARCHIVE_STATUS_COL).value

        article_num = row_idx - 1
        title = ws.cell(row=row_idx, column=13).value or ''

        # Skip rows with no URL
        if not url:
            print(f'[{article_num:4d}/{total_rows}] SKIP — no URL | {title[:60]}')
            skipped += 1
            continue

        # Skip rows already successfully processed
        if existing_status in ('found', 'submitted'):
            print(f'[{article_num:4d}/{total_rows}] SKIP (already {existing_status}) | {title[:60]}')
            skipped += 1
            continue

        print(f'[{article_num:4d}/{total_rows}] Processing | {title[:60]}')
        print(f'      URL: {str(url)[:80]}')

        archive_url = None
        status = 'failed'

        for attempt in range(1, MAX_RETRIES + 1):
            if attempt > 1:
                print(f'      Retry {attempt}/{MAX_RETRIES}...')
                time.sleep(10)

            archive_url, status = get_archive_url(str(url), session)

            if status in ('found', 'submitted'):
                break

        # Write result back to spreadsheet
        ws.cell(row=row_idx, column=ARCHIVE_URL_COL).value = archive_url
        ws.cell(row=row_idx, column=ARCHIVE_STATUS_COL).value = status

        # Save workbook after every row (so progress is never lost)
        wb.save(WORKBOOK_PATH)

        if status in ('found', 'submitted'):
            if status == 'found':
                found += 1
                print(f'      ✓ Found: {archive_url}')
            else:
                submitted += 1
                print(f'      ✓ Submitted: {archive_url}')
            # Note: consecutive_failures and backoff_level already reset inside get_archive_url
            time.sleep(DELAY_EXISTING)
        else:
            failed += 1
            consecutive_failures += 1
            print(f'      ✗ Failed — will need Factiva for this one')
            # Auto-pause if we've hit too many consecutive failures
            if consecutive_failures >= CONSEC_FAIL_THRESHOLD:
                handle_rate_limit()

        print()

    # Final summary
    elapsed = datetime.now() - start_time
    total_success = found + submitted
    print('=' * 60)
    print(f'DONE in {elapsed}')
    print(f'  Already on archive.ph:  {found:4d}')
    print(f'  Newly submitted:        {submitted:4d}')
    print(f'  Failed (need Factiva):  {failed:4d}')
    print(f'  Skipped (no URL):       {skipped:4d}')
    print(f'  Success rate: {total_success}/{total_rows - skipped} = {100*total_success/max(1, total_rows-skipped):.1f}%')
    print(f'\nWorkbook saved to: {WORKBOOK_PATH}')
    print(f'\nNext step: run phase2_llm_coding.py to code all articles with full text.')


if __name__ == '__main__':
    main()
