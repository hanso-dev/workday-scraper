#!/usr/bin/env python3
"""
Middle layer: a small FastAPI app that sits between the frontend (web/index.html)
and workday_scraper.py (the backend). Submit a Workday careers URL from the UI,
watch it scrape with live progress, then search the results in the same page.

    python3 server.py
    -> http://127.0.0.1:8787

Scraped output lands in this directory as <out>.json / <out>.csv, same as
running workday_scraper.py directly from the CLI -- the UI is just another
way to drive it.
"""

import json
import re
import threading
import time
import uuid
from collections import deque
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import workday_scraper as scraper

ROOT = Path(__file__).parent
WEB_DIR = ROOT / "web"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Workday Scraper")

_jobs = {}
_jobs_lock = threading.Lock()


class ScrapeRequest(BaseModel):
    url: str
    out: str | None = None
    workers: int = 24
    ignore_url_filters: bool = False
    no_enrich: bool = False


def _safe_stem(name):
    name = re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_")
    return name or "jobs"


def _run_scrape(job_id, req):
    state = _jobs[job_id]
    try:
        board = scraper.Board(req.url)
    except ValueError as e:
        with _jobs_lock:
            state["phase"] = "error"
            state["error"] = str(e)
        return

    stem = _safe_stem(req.out or board.tenant)
    with _jobs_lock:
        state["stem"] = stem
        state["phase"] = "listing"

    applied_facets = {} if req.ignore_url_filters else board.url_facets
    if applied_facets:
        with _jobs_lock:
            state["log"].append(f"applying filter from URL: {applied_facets}")

    def on_list_progress(fetched, total):
        with _jobs_lock:
            state["list_done"] = fetched
            state["list_total"] = total

    def on_enrich_progress(done, total, status, title):
        with _jobs_lock:
            state["enrich_done"] = done
            state["enrich_total"] = total
            if status != "ok":
                state["log"].append(f"[{done}/{total}] {status}: {title}")

    try:
        session = scraper.make_session(board)
        jobs = scraper.list_jobs(
            session, board, facets=applied_facets, on_progress=on_list_progress, workers=req.workers,
        )

        # resume: merge against any previous output for this stem
        prev_path = DATA_DIR / f"{stem}.json"
        if prev_path.exists():
            try:
                prev_by_path = {
                    j.get("externalPath"): j
                    for j in json.loads(prev_path.read_text())
                    if j.get("externalPath")
                }
                for j in jobs:
                    prev = prev_by_path.get(j.get("externalPath"))
                    if prev and "description" in prev:
                        for k in ("description", "description_html", "error"):
                            if k in prev:
                                j[k] = prev[k]
            except (json.JSONDecodeError, OSError):
                pass

        with _jobs_lock:
            state["phase"] = "enriching"
            state["enrich_total"] = len([j for j in jobs if j.get("externalPath")])

        if not req.no_enrich:
            scraper.enrich(
                session, board, jobs,
                workers=req.workers, stem=str(DATA_DIR / stem), on_progress=on_enrich_progress,
            )
        scraper.save(jobs, str(DATA_DIR / stem))

        with _jobs_lock:
            state["phase"] = "done"
            state["total"] = len(jobs)
    except Exception as e:  # noqa: BLE001
        with _jobs_lock:
            state["phase"] = "error"
            state["error"] = str(e)


@app.post("/api/scrape")
def start_scrape(req: ScrapeRequest):
    try:
        scraper.Board(req.url)  # validate up front so bad URLs fail immediately
    except ValueError as e:
        raise HTTPException(400, str(e))

    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {
        "phase": "starting",
        "url": req.url,
        "stem": None,
        "list_done": 0,
        "list_total": 0,
        "enrich_done": 0,
        "enrich_total": 0,
        "total": 0,
        "error": None,
        "log": deque(maxlen=200),
        "started": time.time(),
    }
    t = threading.Thread(target=_run_scrape, args=(job_id, req), daemon=True)
    t.start()
    return {"job_id": job_id}


@app.get("/api/scrape/{job_id}")
def scrape_status(job_id: str):
    with _jobs_lock:
        state = _jobs.get(job_id)
        if not state:
            raise HTTPException(404, "unknown job id")
        return {**state, "log": list(state["log"])}


@app.get("/api/datasets")
def list_datasets():
    out = []
    for p in sorted(DATA_DIR.glob("*.json")):
        try:
            with open(p) as f:
                head = f.read(4096)
            if not head.lstrip().startswith("["):
                continue
            # cheap check that this looks like scraper output, not some
            # unrelated JSON array lying around in the same directory
            looks_like_jobs = '"externalPath"' in head or ('"title"' in head and '"postedOn"' in head)
            if not looks_like_jobs:
                continue
            size = p.stat().st_size
            out.append({"name": p.name, "size": size, "mtime": p.stat().st_mtime})
        except OSError:
            continue
    return out


@app.get("/api/datasets/{name}")
def get_dataset(name: str):
    if "/" in name or "\\" in name or not name.endswith(".json"):
        raise HTTPException(400, "invalid dataset name")
    path = DATA_DIR / name
    if not path.exists():
        raise HTTPException(404, "not found")
    return FileResponse(path, media_type="application/json")


app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8787)
