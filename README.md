# Workday Scraper

Scrape any public Workday-hosted careers site (`*.myworkdayjobs.com`) and search the results in a browser — live progress, no code changes needed per company.

```
https://td.wd3.myworkdayjobs.com/en-US/TD_Bank_Careers
https://intactfc.wd3.myworkdayjobs.com/en-US/intactfc
https://manulife.wd3.myworkdayjobs.com/MFCJH_Jobs
```
all work with the same tool — the tenant, data center, and site are derived from the URL itself.

## Architecture

Three layers, each replaceable on its own:

```
┌─────────────┐      HTTP (JSON)      ┌──────────────┐      Python calls      ┌───────────────────┐
│  Frontend   │ ───────────────────▶  │  Middle layer │ ───────────────────▶ │  Backend            │
│  web/       │ ◀───────────────────  │  server.py    │ ◀─────────────────── │  workday_scraper.py │
│  index.html │     progress, data    │  (FastAPI)    │   job dicts          │  (requests + CXS)   │
└─────────────┘                       └──────────────┘                      └───────────────────┘
```

- **Backend** (`workday_scraper.py`) — talks to Workday's internal CXS search API directly. No knowledge of HTTP servers or UIs; works standalone as a CLI.
- **Middle layer** (`server.py`) — a FastAPI app that runs the backend in a background thread per scrape request, tracks progress in memory, and serves the frontend + scraped datasets over HTTP.
- **Frontend** (`web/index.html`) — a single static page: submit a URL, poll progress, browse/search/filter the result. No build step, no framework.

`jobs_ui.html` is a fourth, optional piece: a standalone offline viewer with a file picker, for browsing a dataset without running the server at all.

## Quickstart

```bash
cd workday_scraper
pip install -r requirements.txt
python3 server.py
```
Open **http://127.0.0.1:8787**, paste a Workday careers URL, click Scrape.

Or skip the UI and use the backend directly:
```bash
python3 workday_scraper.py https://td.wd3.myworkdayjobs.com/en-US/TD_Bank_Careers
```
Writes `td.json` / `td.csv` to the current directory.

## CLI reference

```
python3 workday_scraper.py <url> [--out NAME] [--workers N] [--no-enrich] [--facets] [--all]
```

| flag | meaning |
|---|---|
| `<url>` | any Workday careers URL. A query string on it (e.g. `?Location_Country=<id>`, copied from a filtered search in the browser) is auto-applied as a search filter. |
| `--out NAME` | output file stem (default: tenant slug derived from the URL). Writes `NAME.json` / `NAME.csv`. |
| `--workers N` | concurrent requests, used for both listing pages and fetching each job's detail page (default 32). |
| `--no-enrich` | list postings only — skip fetching each job's full description (fast). |
| `--facets` | print the board's available filters and their IDs, then exit. Useful for building a filtered URL. |
| `--all` | ignore any filter in the URL's query string and scrape the whole board. |

Re-running the same command resumes: it merges against the previous `NAME.json` and only re-fetches jobs that are missing or previously failed.

**On speed:** Workday's CXS API hard-caps the page size at 20 jobs per request — asking for more (e.g. `limit: 50`) returns an HTTP 400, so that number can't be raised. What *is* adjustable is concurrency: both the listing pages and the per-job detail fetches are fetched `--workers` at a time rather than one at a time, so a bigger board doesn't mean a linearly longer wait. Raise `--workers` for a faster scrape on a large board; lower it if a tenant starts rate-limiting you.

## Data model

### Raw record (`workday_scraper.py` output, one dataset item)

This is Workday's own `jobPosting` object from the CXS API, plus the fields `enrich()` adds after visiting each job's detail page. Field presence varies slightly by tenant — treat everything but `title` and `externalPath` as optional.

| field | type | source | notes |
|---|---|---|---|
| `title` | str | listing | job title |
| `externalPath` | str | listing | path on the tenant's site; stable per-job identifier, used for dedup and resume |
| `externalUrl` | str | detail | absolute URL to the public posting (constructed if the tenant doesn't supply one) |
| `locationsText` | str | listing | e.g. `"2 Locations"` or a single place name |
| `location` | str | detail | primary location, once enriched |
| `postedOn` | str | listing | Workday's relative string: `"Posted Today"`, `"Posted 3 Days Ago"`, `"Posted 30+ Days Ago"` |
| `remoteType` | str | listing | e.g. `"On Site"`, `"Hybrid"`, `"Remote"` — **not populated by every tenant** |
| `bulletFields` | list[str] | listing | tenant-defined chips, usually `[requisition_id, ...]` |
| `jobReqId` | str | detail | requisition ID, when the tenant exposes it separately |
| `timeType` | str | detail | e.g. `"Full time"` |
| `startDate` / `endDate` | str | detail | ISO dates, when present |
| `description` | str | enrich | plain-text job description (HTML tags stripped) |
| `description_html` | str | enrich | raw HTML description from the detail endpoint |
| `id`, `jobPostingId`, `jobPostingSiteId`, `questionnaireId`, `canApply`, `includeResumeParsing`, `timeLeftToApply`, `jobPostingEndDateAsText` | varies | detail | passthrough scalar fields from Workday, kept as-is |
| `error` | str | enrich | present only if the detail fetch ultimately failed after retries |

### Normalized record (what the frontend renders)

Both `web/index.html` and `jobs_ui.html` normalize the raw record into a flatter shape client-side (see `normalize()` in each file) before rendering:

```ts
{
  title: string,
  location: string,       // from locationsText / location
  remote: string,          // from remoteType, "" if absent
  postedOn: string | null, // raw Workday string, shown as-is
  daysAgo: number | null,  // parsed from postedOn, e.g. "Posted 3 Days Ago" -> 3
  reqId: string | null,    // from jobReqId or bulletFields[0]
  timeType: string,
  url: string,              // externalUrl or externalPath
  desc: string               // plain-text description
}
```
`daysAgo` is what powers the "posted within" filter and the sort order; it's recomputed against the real current date every time the page loads, not baked into the stored dataset.

### Scrape job state (`server.py`, in-memory, one per `POST /api/scrape`)

```ts
{
  phase: "starting" | "listing" | "enriching" | "done" | "error",
  url: string,
  stem: string | null,       // output file stem, set once the URL is parsed
  list_done: number, list_total: number,
  enrich_done: number, enrich_total: number,
  total: number,               // final job count, once done
  error: string | null,
  log: string[],                // last 200 lines (warnings, filter info)
  started: number                // unix timestamp
}
```

## API reference (`server.py`)

| method & path | body / params | returns |
|---|---|---|
| `POST /api/scrape` | `{url, out?, workers?, ignore_url_filters?, no_enrich?}` | `{job_id}` — starts a background scrape |
| `GET /api/scrape/{job_id}` | — | current job state (see above), poll this until `phase` is `done` or `error` |
| `GET /api/datasets` | — | `[{name, size, mtime}, ...]` — every `data/*.json` that looks like scraper output |
| `GET /api/datasets/{name}` | — | the raw dataset JSON (array of raw records) |
| `GET /` | — | the frontend (`web/index.html`), and its static assets |

## Project layout

```
workday_scraper/
├── workday_scraper.py   backend: Board URL-parsing, list_jobs, enrich, save, CLI
├── server.py              middle layer: FastAPI app, job tracking, static file serving
├── web/index.html          frontend: scrape form, progress bar, search/filter/sort UI
├── jobs_ui.html             standalone offline viewer (open directly, no server)
├── data/                     scraped output lands here (gitignored)
├── legacy/                    earlier one-tenant-per-script versions, kept for reference
├── requirements.txt
└── LICENSE
```

## Scraping other sites later

The three-layer split exists so the middle layer and frontend don't need to change if the backend grows beyond Workday. If you add a scraper for another ATS (Greenhouse, Lever, iCIMS, etc.):

- Give it the same shape as `workday_scraper.py`: a function that returns a flat list of dicts with at least `title` and some stable per-job identifier, and a `save()` that writes `<stem>.json` / `<stem>.csv` into `data/`.
- The frontend's `normalize()` function already tolerates missing fields (it falls back gracefully), so a new backend doesn't need to match Workday's exact field names — just update `normalize()` if the new source's raw field names differ enough to need remapping.
- `server.py` would need a small dispatch step (detect which backend a URL belongs to, e.g. by hostname pattern) instead of always importing `workday_scraper` — that's the one real change, everything else (progress tracking, dataset listing, the search UI) is already source-agnostic.

## Roadmap

- **Natural-language search** — today's keyword search matches literal substrings in the description. Planned: a search mode that understands requirements/experience phrased naturally (e.g. "5+ years Python, no degree required") and matches against what the description actually asks for, not just shared words.

## License

MIT — see [LICENSE](LICENSE).
