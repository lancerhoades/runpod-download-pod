import os
import sys
import json
import time
import pathlib
import requests
import runpod
from typing import Optional, Callable
from requests.exceptions import ReadTimeout, ConnectionError as ReqConnError

# -------- Config --------
VOLUME_PATH = os.getenv("RUNPOD_VOLUME_PATH", "/runpod-volume")
VIMEO_API = "https://api.vimeo.com"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# Tuning knobs (override via endpoint env if needed)
MAX_HEIGHT        = int(os.getenv("MAX_HEIGHT", "1080"))   # prefer <= this height
PRINT_EVERY_MB    = int(os.getenv("PRINT_EVERY_MB", "100"))
DL_CHUNK_MB       = int(os.getenv("DL_CHUNK_MB", "4"))     # chunk size in MB
DL_CONNECT_TO     = int(os.getenv("DL_CONNECT_TIMEOUT", "30"))
DL_READ_TO        = int(os.getenv("DL_READ_TIMEOUT", "60"))  # per-chunk read timeout
DL_MAX_RETRIES    = int(os.getenv("DL_MAX_RETRIES", "20"))
DL_BACKOFF_FACTOR = float(os.getenv("DL_BACKOFF_FACTOR", "1.7"))

def ensure_env():
    if not VIMEO_ACCESS_TOKEN:
        raise RuntimeError("VIMEO_ACCESS_TOKEN env var is required (set it on the endpoint).")
    if not os.path.isdir(VOLUME_PATH):
        raise RuntimeError(
            f"RunPod network volume is not mounted at {VOLUME_PATH}. "
            "In Serverless, attach a Network Volume in Endpoint → Advanced → Network Volume."
        )

def mkdir_p(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def vimeo_get(path: str, params=None, fields=None):
    headers = {
        "Authorization": f"bearer {VIMEO_ACCESS_TOKEN}",
        "Accept": "application/vnd.vimeo.*+json;version=3.4",
    }
    params = params or {}
    if fields:
        params["fields"] = fields
    url = f"{VIMEO_API}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Vimeo API GET failed {r.status_code}: {r.text[:300]}")
    return r.json()

def choose_best_mp4(video_json: dict):
    """
    Build candidates from `download` and `files` lists.
    Prefer highest resolution <= MAX_HEIGHT. If none, fall back to highest.
    """
    candidates = []  # (height_or_width, height, link)
    # download[]
    for d in (video_json.get("download") or []):
        link = d.get("link")
        mime = (d.get("type") or "").lower()
        h = d.get("height") or 0
        w = d.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime):
            candidates.append((h or w, h, link))
    # files[]
    for f in (video_json.get("files") or []):
        link = f.get("link")
        mime = (f.get("type") or "").lower()
        h = f.get("height") or 0
        w = f.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime):
            candidates.append((h or w, h, link))

    if not candidates:
        raise RuntimeError("No progressive MP4 candidates found in Vimeo response.")

    # Sort by “resolution” (height or width) desc
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Prefer <= MAX_HEIGHT if we have heights
    under = [c for c in candidates if c[1] and c[1] <= MAX_HEIGHT]
    if under:
        return under[0][2]
    return candidates[0][2]

def stream_download_resume(
    url: str,
    dest_path: str,
    *,
    refresh_url: Optional[Callable[[], str]] = None,
    chunk_bytes: int = None,
    connect_timeout: int = DL_CONNECT_TO,
    read_timeout: int = DL_READ_TO,
    max_retries: int = DL_MAX_RETRIES,
    backoff_factor: float = DL_BACKOFF_FACTOR,
) -> int:
    """
    Resumable download with HTTP Range, retries, and optional URL refresh.
    """
    chunk_bytes = chunk_bytes or (DL_CHUNK_MB * 1024 * 1024)
    downloaded = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
    next_report = ((downloaded // (PRINT_EVERY_MB * 1024 * 1024)) + 1) * (PRINT_EVERY_MB * 1024 * 1024)

    def _req(u: str, start: int):
        headers = {"Accept-Encoding": "identity"}
        if start > 0:
            headers["Range"] = f"bytes={start}-"
        return requests.get(u, stream=True, headers=headers, timeout=(connect_timeout, read_timeout))

    retries = 0
    while True:
        try:
            r = _req(url, downloaded)
            if r.status_code == 416:
                # Already fully downloaded
                break
            if r.status_code not in (200, 206):
                # Possibly expired URL; try refresh if available
                if r.status_code in (401, 403, 404, 410) and refresh_url and retries < max_retries:
                    retries += 1
                    sleep_s = backoff_factor ** retries
                    print(f"[DL] Got {r.status_code}, refreshing URL (retry {retries}) after {sleep_s:.1f}s", flush=True)
                    time.sleep(sleep_s)
                    url = refresh_url()
                    continue
                raise RuntimeError(f"Download HTTP {r.status_code}: {r.text[:300]}")

            mode = "ab" if downloaded > 0 else "wb"
            with open(dest_path, mode) as f:
                for chunk in r.iter_content(chunk_size=chunk_bytes):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= next_report:
                        mb = downloaded // (1024 * 1024)
                        print(f"[DL] {mb} MB downloaded...", flush=True)
                        next_report += PRINT_EVERY_MB * 1024 * 1024

            # Completed this connection; if server closed cleanly we should be done.
            # Verify if server gave Content-Range; if not, assume done.
            # Most CDNs will have sent the remainder, so we can break.
            break

        except (ReadTimeout, ReqConnError) as e:
            if retries >= max_retries:
                raise RuntimeError(f"Download failed after {retries} retries: {e}")
            retries += 1
            sleep_s = backoff_factor ** retries
            print(f"[DL] Stall/timeout, will resume from {downloaded} after {sleep_s:.1f}s (retry {retries})", flush=True)
            time.sleep(sleep_s)
            # Optionally refresh the URL each time we hit a network error
            if refresh_url:
                try:
                    url = refresh_url()
                except Exception as re:
                    print(f"[DL] URL refresh failed, keeping old URL: {re}", flush=True)
            continue

    size = os.path.getsize(dest_path)
    return size

def handler(event):
    """
    input: { "job_id": "...", "vimeo_id": "1114674380" }
    output: { "ok": true, "path": "<VOLUME>/job_id/raw/full.mp4", "bytes": N, "source_url": "..." }
    """
    ensure_env()

    payload = (event.get("input") or {}) if isinstance(event, dict) else {}
    job_id = payload.get("job_id")
    vimeo_id = payload.get("vimeo_id") or payload.get("video_id")

    if not job_id:
        raise RuntimeError("Missing 'job_id' in input.")
    if not vimeo_id:
        raise RuntimeError("Missing 'vimeo_id' (or 'video_id') in input.")

    raw_dir = f"{VOLUME_PATH}/{job_id}/raw"
    mkdir_p(raw_dir)
    out_path = f"{raw_dir}/full.mp4"

    print(f"[INFO] Starting download | job_id={job_id} vimeo_id={vimeo_id} dest={out_path}", flush=True)
    fields = "download.link,download.type,download.height,download.width,files.link,files.type,files.height,files.width,name,uri"
    vj = vimeo_get(f"/videos/{vimeo_id}", fields=fields)
    mp4_url = choose_best_mp4(vj)
    print(f"[INFO] Selected MP4: {mp4_url}", flush=True)

    def refresh_url():
        vj2 = vimeo_get(f"/videos/{vimeo_id}", fields=fields)
        return choose_best_mp4(vj2)

    size = stream_download_resume(
        mp4_url,
        out_path,
        refresh_url=refresh_url
    )
    print(f"[INFO] Finished download: {size} bytes -> {out_path}", flush=True)

    meta_dir = f"{VOLUME_PATH}/{job_id}/metadata"
    mkdir_p(meta_dir)
    job_json = {
        "job_id": job_id,
        "vimeo_id": vimeo_id,
        "downloaded_at_utc": int(time.time()),
        "raw_full_path": out_path,
        "bytes": size,
        "source_url": mp4_url,
        "video_uri": vj.get("uri"),
        "name": vj.get("name"),
    }
    with open(f"{meta_dir}/job.json", "w", encoding="utf-8") as f:
        json.dump(job_json, f, ensure_ascii=False, indent=2)

    return {"ok": True, "path": out_path, "bytes": size, "source_url": mp4_url}

runpod.serverless.start({"handler": handler})
