#!/usr/bin/env python3
"""
Generic Workday CXS scraper. Point it at any *.myworkdayjobs.com careers URL
and it derives the tenant/site/data-center from it -- no per-company script needed.

    python3 workday_scraper.py https://td.wd3.myworkdayjobs.com/en-US/TD_Bank_Careers
    python3 workday_scraper.py https://intactfc.wd3.myworkdayjobs.com/en-US/intactfc --out intact

If the URL has a query string (e.g. copied from a filtered search in the
browser, like ?Location_Country=<id>), it's applied as a search facet
automatically -- pass --all to ignore it and scrape everything instead.

Writes <out>.json / <out>.csv (out defaults to the tenant slug). The json is
a flat list of jobPosting dicts (+ description/description_html once enriched);
point jobs_ui.html at it to browse.
"""

import argparse
import csv
import html
import json
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qs, urlparse

import requests

_SCALAR = (str, int, float, bool, type(None))

log = logging.getLogger("workday_scraper")
if not log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)

_HOST_RE = re.compile(r"^https://(?P<tenant>[^.]+)\.(?P<dc>wd\d+)\.myworkdayjobs\.com")
# locale segment (en-US, fr-CA, ...) is optional -- some tenants skip it
_WITH_LOCALE_RE = re.compile(r"^/(?P<locale>[a-z]{2}-[A-Z]{2})/(?P<site>[^/]+)/?(?:jobs/?)?$")
_NO_LOCALE_RE = re.compile(r"^/(?P<site>[^/]+)/?(?:jobs/?)?$")


class Board:
    """Derived endpoints for one Workday careers site."""

    def __init__(self, url):
        url = url.strip()
        parsed = urlparse(url)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        # query params on a Workday careers URL are applied search facets, e.g.
        # ?Location_Country=<id> -- the param name matches the facetParameter
        # the CXS search API expects, and each value can repeat for multi-select.
        self.url_facets = {k: v for k, v in parse_qs(parsed.query).items()}

        hm = _HOST_RE.match(clean_url)
        if not hm:
            raise ValueError(
                f"doesn't look like a Workday careers URL: {url!r}\n"
                "expected something like https://<tenant>.wd3.myworkdayjobs.com/en-US/<site>"
            )
        path = clean_url[hm.end():] or "/"
        m = _WITH_LOCALE_RE.match(path)
        locale = m.group("locale") if m else None
        if not m:
            m = _NO_LOCALE_RE.match(path)
        if not m:
            raise ValueError(f"couldn't find a site name in the URL path: {path!r}")

        g = hm.groupdict()
        self.tenant = g["tenant"]
        self.host = f"https://{g['tenant']}.{g['dc']}.myworkdayjobs.com"
        self.site = m.group("site")
        self.locale = locale
        self.cxs = f"{self.host}/wday/cxs/{self.tenant}/{self.site}"
        prefix = f"{self.host}/{self.locale}/{self.site}" if self.locale else f"{self.host}/{self.site}"
        self.html_jobs = f"{prefix}/jobs"
        self.external_base = prefix


def make_session(board):
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:154.0) "
                "Gecko/20100101 Firefox/154.0"
            ),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": board.host,
            "Referer": board.html_jobs,
        }
    )
    s.get(board.html_jobs, timeout=30)
    return s


_PAGE_SIZE = 20  # Workday's CXS API hard-caps `limit` at 20; anything higher is an HTTP 400


def list_jobs(session, board, facets=None, retries=4, on_progress=None, workers=8):
    """Page /jobs (Workday caps the page size at 20 -- see _PAGE_SIZE) and return a
    flat list of jobPosting dicts. Pages after the first are fetched concurrently
    (`workers` at a time), since each page is an independent request once the
    total is known. `on_progress(fetched, total)` is called as pages land."""

    def post(offset):
        payload = {"appliedFacets": facets or {}, "limit": _PAGE_SIZE, "offset": offset, "searchText": ""}
        for attempt in range(retries):
            r = session.post(board.cxs + "/jobs", json=payload, timeout=30)
            try:
                if r.status_code == 200:
                    return r.json()
            except ValueError:
                pass
            log.warning("list offset %d: HTTP %d, retry %d", offset, r.status_code, attempt + 1)
            time.sleep(2**attempt)
            session.get(board.html_jobs, timeout=30)
        r.raise_for_status()
        raise RuntimeError(f"list failed at offset {offset}")

    first = post(0)
    total = first.get("total", 0)
    log.info("listing %d jobs", total)
    pages = {0: first.get("jobPostings", [])}
    fetched = len(pages[0])
    if on_progress:
        on_progress(fetched, total)

    offsets = list(range(_PAGE_SIZE, total, _PAGE_SIZE))
    if offsets:
        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(post, offset): offset for offset in offsets}
            for fut in as_completed(futs):
                offset = futs[fut]
                try:
                    batch = fut.result().get("jobPostings", [])
                except Exception as e:  # noqa: BLE001
                    log.warning("list offset %d failed: %s", offset, e)
                    batch = []
                with lock:
                    pages[offset] = batch
                    fetched += len(batch)
                    if on_progress:
                        on_progress(fetched, total)

    jobs = []
    for offset in sorted(pages):
        jobs.extend(pages[offset])
    return jobs


def _text(raw):
    """HTML -> readable plain text, no dependencies."""
    raw = re.sub(r"(?i)</p>|<br\s*/?>|</li>", "\n", raw)
    raw = re.sub(r"(?i)<li[^>]*>", "• ", raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    raw = html.unescape(raw)
    return re.sub(r"\n{3,}", "\n\n", raw).strip()


def _fetch_one(session, board, job, retries):
    """Fill `job` in place from its detail endpoint. Returns 'ok' | 'fail' | 'skip'."""
    path = job.get("externalPath")
    if not path:
        return "skip"
    if "externalUrl" not in job:
        job["externalUrl"] = board.external_base + path
    for attempt in range(retries):
        try:
            r = session.get(board.cxs + path, headers={"Accept": "application/json"}, timeout=30)
            if r.status_code in (403, 429, 503):
                time.sleep(2**attempt * 2 + attempt)
                session.get(board.html_jobs, timeout=30)
                continue
            r.raise_for_status()
            info = r.json().get("jobPostingInfo", {})
            job["description_html"] = info.get("jobDescription", "")
            job["description"] = _text(job["description_html"])
            job.pop("error", None)
            for k, v in info.items():
                if k != "jobDescription" and isinstance(v, _SCALAR):
                    job.setdefault(k, v)
            return "ok"
        except Exception as e:  # noqa: BLE001
            if attempt == retries - 1:
                job["error"] = str(e)
                return "fail"
            time.sleep(2**attempt * 2)
    job["error"] = "exhausted retries"
    return "fail"


def enrich(session, board, jobs, workers=32, retries=3, checkpoint=500, stem="jobs", skip_done=True, on_progress=None):
    """`jobs` = iterable of job dicts (each needs `externalPath`). Mutates in place.
    Re-runnable: with skip_done, jobs that already have a `description` and no
    `error` are left alone. `on_progress(done, total, status, title)` is called
    after each job, if given."""
    try:
        from requests.adapters import HTTPAdapter

        session.mount("https://", HTTPAdapter(pool_connections=workers, pool_maxsize=workers))
    except Exception:  # noqa: BLE001
        pass

    jobs = list(jobs)
    todo = [
        j for j in jobs
        if j.get("externalPath") and not (skip_done and "description" in j and "error" not in j)
    ]
    total = len(todo)
    if not total:
        log.info("enrich: nothing to do (%d jobs already enriched)", len(jobs))
        return jobs

    started = time.monotonic()
    lock = threading.Lock()
    counts = {"ok": 0, "fail": 0, "skip": 0}
    done = 0
    log.info("enrich: %d/%d jobs, %d workers", total, len(jobs), workers)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_fetch_one, session, board, j, retries): j for j in todo}
        for fut in as_completed(futs):
            j = futs[fut]
            try:
                status = fut.result()
            except Exception as e:  # noqa: BLE001
                status = "fail"
                j["error"] = str(e)

            with lock:
                counts[status] = counts.get(status, 0) + 1
                done += 1
                n = done

            elapsed = time.monotonic() - started
            rate = n / elapsed if elapsed else 0
            eta = (total - n) / rate if rate else 0
            level = log.info if status == "ok" else log.warning
            title = j.get("title") or j.get("externalPath") or ""
            level(
                "[%4d/%d] %-4s %5.1f/s eta %dm%02ds  ok=%d fail=%d  %s",
                n, total, status, rate, eta // 60, eta % 60,
                counts["ok"], counts["fail"], title[:55],
            )
            if on_progress:
                on_progress(n, total, status, title)

            if checkpoint and n % checkpoint == 0:
                with lock:
                    save(jobs, stem, quiet=True)
                log.info("  ~ checkpoint written at %d", n)

    save(jobs, stem)
    log.info(
        "enrich done in %.0fs -- ok=%d fail=%d skip=%d",
        time.monotonic() - started, counts["ok"], counts["fail"], counts["skip"],
    )
    if counts["fail"]:
        log.info("  re-run the same command to retry the %d failures", counts["fail"])
    return jobs


def save(pages, stem="jobs", quiet=False):
    """`pages` is any iterable of jobPosting dicts, or an iterable of such lists."""
    jobs = []
    for item in pages:
        jobs.extend(item) if isinstance(item, list) else jobs.append(item)

    seen, unique = set(), []
    for j in jobs:
        key = j.get("externalPath") or json.dumps(j, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(j)
    jobs = unique

    with open(f"{stem}.json", "w") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    cols = list(dict.fromkeys(k for j in jobs for k in j))
    with open(f"{stem}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for j in jobs:
            w.writerow(
                {
                    k: v if isinstance(v, _SCALAR) else json.dumps(v, ensure_ascii=False)
                    for k, v in j.items()
                }
            )

    if not quiet:
        log.info("wrote %d jobs -> %s.json / %s.csv", len(jobs), stem, stem)
    return jobs


def list_facets(board):
    """Dump available facet ids/values -- use these to partition if you
    hit the deep-pagination wall."""
    session = make_session(board)
    payload = {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""}
    data = session.post(board.cxs + "/jobs", json=payload, timeout=30).json()
    for facet in data.get("facets", []):
        print(f"\n{facet.get('facetParameter')}  ({facet.get('descriptor')})")
        for v in facet.get("values", [])[:40]:
            print(f"  {v.get('id')!r:45} {v.get('descriptor')} ({v.get('count')})")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url", help="a Workday careers URL, e.g. https://td.wd3.myworkdayjobs.com/en-US/TD_Bank_Careers")
    ap.add_argument("--out", help="output stem (default: tenant slug)")
    ap.add_argument("--workers", type=int, default=32, help="concurrent detail-page fetches (default 32)")
    ap.add_argument("--no-enrich", action="store_true", help="skip fetching full descriptions, just list postings")
    ap.add_argument("--facets", action="store_true", help="print available search facets and exit")
    ap.add_argument(
        "--all", action="store_true",
        help="ignore any filter facets in the URL's query string and scrape everything",
    )
    args = ap.parse_args()

    board = Board(args.url)
    if args.facets:
        list_facets(board)
        return

    stem = args.out or board.tenant
    applied_facets = {} if args.all else board.url_facets
    if applied_facets:
        log.info("applying filter from URL: %s", applied_facets)

    session = make_session(board)
    jobs = list_jobs(session, board, facets=applied_facets, workers=args.workers)

    try:
        with open(f"{stem}.json") as f:
            prev_by_path = {j.get("externalPath"): j for j in json.load(f) if j.get("externalPath")}
        for j in jobs:
            prev = prev_by_path.get(j.get("externalPath"))
            if prev and "description" in prev:
                for k in ("description", "description_html", "error"):
                    if k in prev:
                        j[k] = prev[k]
        log.info("loaded %d previously enriched jobs from %s.json for resume", len(prev_by_path), stem)
    except FileNotFoundError:
        pass

    if not args.no_enrich:
        enrich(session, board, jobs, workers=args.workers, stem=stem)
    save(jobs, stem)


if __name__ == "__main__":
    main()
