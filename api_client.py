"""Thin client for the url2src MAX API (https://url-to-src-api.onrender.com).
All calls are blocking — call them with asyncio.to_thread() from bot handlers."""

import threading
import time
from io import BytesIO

import requests

import config

API_BASE_URL = str(getattr(config, "API_BASE_URL", "")).strip().rstrip("/")
API_KEY = str(getattr(config, "API_KEY", "")).strip()
KEEPALIVE_SECONDS = int(getattr(config, "API_KEEPALIVE_SECONDS", 600))

# Render free instances sleep when idle; a cold start can take up to ~60s.
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 90
RETRY_STATUSES = (502, 503, 504)


class ApiError(Exception):
    def __init__(self, message: str, status: int = None):
        super().__init__(message)
        self.status = status


def configured() -> bool:
    return bool(API_BASE_URL and API_KEY) and "YOUR-API" not in API_BASE_URL


def _headers():
    return {"x-api-key": API_KEY}


def _request(method, path, *, params=None, json=None, stream=False, retries=4, timeout=None):
    if not configured():
        raise ApiError("API is not configured (set API_BASE_URL and API_KEY in config.py).")
    last = None
    for attempt in range(retries):
        try:
            r = requests.request(
                method, API_BASE_URL + path,
                headers=_headers(),
                params=params, json=json, stream=stream,
                timeout=timeout or (CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            last = ApiError(f"API unreachable: {e.__class__.__name__}")
            time.sleep(4)
            continue
        if r.status_code in RETRY_STATUSES and attempt < retries - 1:
            r.close()
            last = ApiError(f"API busy/starting (HTTP {r.status_code})", r.status_code)
            time.sleep(5)
            continue
        if r.status_code >= 400:
            try:
                msg = r.json().get("error") or r.text[:200]
            except ValueError:
                msg = r.text[:200]
            status = r.status_code
            r.close()
            raise ApiError(msg or f"HTTP {status}", status)
        return r
    raise last or ApiError("API request failed.")


def start_job(url: str, **opts) -> dict:
    """opts are passed straight through to the API as job options — e.g.
    engine, speed, crawl, max_pages, screenshot, snapshot, assets,
    sourcemaps, auto_unlock. See the API's own README for the full list."""
    body = {"url": url}
    body.update(opts)
    return _request("POST", "/api/jobs", json=body, retries=2).json()


def job_status(job_id: str, since: int = 0) -> dict:
    """Returns {job_id, status, error, result, events}; `events` holds only
    the events after `since` (pass back the running total you've already
    consumed to poll incrementally instead of re-reading the whole log)."""
    return _request("GET", f"/api/jobs/{job_id}", params={"since": since}, retries=2).json()


def _download(path: str, max_bytes: int) -> BytesIO:
    r = _request("GET", path, stream=True)
    try:
        length = r.headers.get("Content-Length")
        if length and length.isdigit() and int(length) > max_bytes:
            raise ApiError("Result is larger than Telegram's upload limit.")
        buf = BytesIO()
        for chunk in r.iter_content(chunk_size=256 * 1024):
            buf.write(chunk)
            if buf.tell() > max_bytes:
                raise ApiError("Result is larger than Telegram's upload limit.")
        buf.seek(0)
        return buf
    finally:
        r.close()


def download_zip(job_id: str, max_bytes: int) -> BytesIO:
    return _download(f"/api/jobs/{job_id}/zip", max_bytes)


def download_file(job_id: str, name: str, max_bytes: int) -> BytesIO:
    """Pulls one file out of the job's zip without downloading the whole
    zip — e.g. name='screenshot.jpg', 'snapshot.html', 'meta.json',
    'links.json'."""
    return _download(f"/api/jobs/{job_id}/file/{name}", max_bytes)


def wake() -> bool:
    """Ping /health (no key needed) — also wakes a sleeping Render instance."""
    if not API_BASE_URL:
        return False
    try:
        return requests.get(API_BASE_URL + "/health", timeout=(CONNECT_TIMEOUT, 90)).ok
    except requests.RequestException:
        return False


def start_keepalive():
    """Pings the API periodically so a free-tier instance doesn't go to sleep."""
    if not API_BASE_URL or KEEPALIVE_SECONDS <= 0:
        return

    def loop():
        while True:
            time.sleep(KEEPALIVE_SECONDS)
            wake()

    threading.Thread(target=loop, daemon=True).start()
