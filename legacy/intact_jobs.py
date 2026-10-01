"""Generic Workday CXS scraper, pointed at Intact Financial Corp's board.
Reuses the same shape as td_jobs.py, adapted for a different tenant.
"""
import csv
import html
import json
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

CXS = "https://intactfc.wd3.myworkdayjobs.com/wday/cxs/intactfc/intactfc"
HTML_JOBS = "https://intactfc.wd3.myworkdayjobs.com/en-US/intactfc"

_SCALAR = (str, int, float, bool, type(None))

log = logging.getLogger("intact_jobs")
if not log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)


def list_jobs(session, facets=None, retries=4):
    def post(offset):
        payload = {"appliedFacets": facets or {}, "limit": 20, "offset": offset, "searchText": ""}
        for attempt in range(retries):
            r = session.post(CXS + "/jobs", json=payload, timeout=30)
            try:
                if r.status_code == 200:
                    return r.json()
            except ValueError:
                pass
            log.warning("list offset %d: HTTP %d, retry %d", offset, r.status_code, attempt + 1)
            time.sleep(2**attempt)
            session.get(HTML_JOBS, timeout=30)
        r.raise_for_status()
        raise RuntimeError(f"list failed at offset {offset}")

    first = post(0)
    total = first.get("total", 0)
    log.info("listing %d jobs", total)
    jobs = list(first.get("jobPostings", []))
    for offset in range(20, total, 20):
        batch = post(offset).get("jobPostings", [])
        if not batch:
            log.warning("empty page at offset %d, stopping (%d/%d)", offset, len(jobs), total)
            break
        jobs.extend(batch)
    return jobs


def _text(raw):
    raw = re.sub(r"(?i)</p>|<br\s*/?>|</li>", "\n", raw)
    raw = re.sub(r"(?i)<li[^>]*>", "• ", raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    raw = html.unescape(raw)
    return re.sub(r"\n{3,}", "\n\n", raw).strip()


def _fetch_one(session, job, retries):
    path = job.get("externalPath")
    if not path:
        return "skip"
    for attempt in range(retries):
        try:
            r = session.get(CXS + path, headers={"Accept": "application/json"}, timeout=30)
            if r.status_code in (403, 429, 503):
                time.sleep(2**attempt * 2 + attempt)
                session.get(HTML_JOBS, timeout=30)
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


def enrich(session, jobs, workers=32, retries=3, checkpoint=500, stem="intact_jobs", skip_done=True):
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
        futs = {pool.submit(_fetch_one, session, j, retries): j for j in todo}
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
            level(
                "[%4d/%d] %-4s %5.1f/s eta %dm%02ds  ok=%d fail=%d  %s",
                n, total, status, rate, eta // 60, eta % 60,
                counts["ok"], counts["fail"], (j.get("title") or j.get("externalPath") or "")[:55],
            )

            if checkpoint and n % checkpoint == 0:
                with lock:
                    save(jobs, stem, quiet=True)
                log.info("  ~ checkpoint written at %d", n)

    save(jobs, stem)
    log.info(
        "enrich done in %.0fs -- ok=%d fail=%d skip=%d",
        time.monotonic() - started, counts["ok"], counts["fail"], counts["skip"],
    )
    return jobs


def save(pages, stem="intact_jobs", quiet=False):
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


if __name__ == "__main__":
    s = requests.Session()
    s.get(HTML_JOBS, timeout=30)

    jobs = list_jobs(s)
    enrich(s, jobs, workers=32)
    save(jobs)
