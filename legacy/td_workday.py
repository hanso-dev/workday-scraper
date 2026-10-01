#!/usr/bin/env python3
"""
Scrape all job postings from TD Bank's Workday CXS endpoint.

Workday caps `limit` at 20, so pagination is done via `offset`.
Outputs td_jobs.json and td_jobs.csv.
"""

import csv
import json
import sys
import time

import requests

TENANT = "https://td.wd3.myworkdayjobs.com"
SITE = "/wday/cxs/td/TD_Bank_Careers/jobs"
HTML = "/en-US/TD_Bank_Careers/jobs"

PAGE = 500  # server-side maximum
DELAY = 0.25  # seconds between requests
MAX_RETRIES = 3

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:154.0) "
    "Gecko/20100101 Firefox/154.0"
)


def make_session():
    """Prime a session so Cloudflare sets __cf_bm / _cfuvid itself."""
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": TENANT,
            "Referer": TENANT + HTML,
        }
    )
    s.get(TENANT + HTML, timeout=30)
    return s


def fetch_page(session, offset, facets=None):
    payload = {
        "appliedFacets": facets or {},
        "limit": PAGE,
        "offset": offset,
        "searchText": "",
    }
    for attempt in range(MAX_RETRIES):
        r = session.post(TENANT + SITE, json=payload, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (403, 429, 503):
            # Cloudflare challenge or rate limit -- back off and re-prime
            time.sleep(2**attempt * 3)
            session.get(TENANT + HTML, timeout=30)
            continue
        r.raise_for_status()
    raise RuntimeError(f"failed at offset {offset}: HTTP {r.status_code}")


def flatten(p):
    return {
        "title": p.get("title"),
        "req_id": (p.get("bulletFields") or [None])[0],
        "location": p.get("locationsText"),
        "posted": p.get("postedOn"),
        "path": p.get("externalPath"),
        "url": TENANT + "/en-US/TD_Bank_Careers" + (p.get("externalPath") or ""),
    }


def scrape_all(facets=None):
    session = make_session()
    first = fetch_page(session, 0, facets)
    total = first.get("total", 0)
    print(f"total reported: {total}", file=sys.stderr)

    jobs = [flatten(p) for p in first.get("jobPostings", [])]
    seen = {j["path"] for j in jobs}

    offset = PAGE
    while offset < total:
        data = fetch_page(session, offset, facets)
        batch = data.get("jobPostings", [])
        if not batch:
            print(
                f"empty page at offset {offset} -- stopping early "
                f"({len(jobs)}/{total})",
                file=sys.stderr,
            )
            break
        new = 0
        for p in batch:
            f = flatten(p)
            if f["path"] not in seen:
                seen.add(f["path"])
                jobs.append(f)
                new += 1
        print(f"offset {offset}: +{new} (total {len(jobs)})", file=sys.stderr)
        offset += PAGE
        time.sleep(DELAY)

    return jobs, total


def list_facets():
    """Dump available facet ids/values -- use these to partition if you
    hit the deep-pagination wall."""
    session = make_session()
    data = fetch_page(session, 0)
    for facet in data.get("facets", []):
        print(f"\n{facet.get('facetParameter')}  ({facet.get('descriptor')})")
        for v in facet.get("values", [])[:40]:
            print(f"  {v.get('id')!r:45} {v.get('descriptor')} ({v.get('count')})")


if __name__ == "__main__":
    if "--facets" in sys.argv:
        list_facets()
        sys.exit(0)

    jobs, total = scrape_all()

    with open("td_jobs.json", "w") as f:
        json.dump(jobs, f, indent=2)

    with open("td_jobs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(jobs[0].keys()))
        w.writeheader()
        w.writerows(jobs)

    print(
        f"\nwrote {len(jobs)} of {total} to td_jobs.json / td_jobs.csv", file=sys.stderr
    )
