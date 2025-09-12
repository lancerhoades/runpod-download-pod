import os
import json
import time
import pathlib
import requests
import runpod

VIMEO_API = "https://api.vimeo.com"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# -------- Helpers --------

def ensure_env():
    if not VIMEO_ACCESS_TOKEN:
        raise RuntimeError("VIMEO_ACCESS_TOKEN env var is required (set it in your RunPod template).")
    if not os.path.isdir("/storage"):
        raise RuntimeError("RunPod /storage is not mounted. Attach a Network Volume at /storage.")

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
        raise RuntimeError(f"Vimeo API GET failed {r.status_code}: {r.text}")
    return r.json()

def choose_best_mp4(video_json: dict):
    """
    Vimeo returns either:
      - 'download': list of downloadable files (often MP4)
      - 'files': list with progressive or HLS variants

    Strategy:
      1) Prefer 'download' items with mp4-ish mime types
      2) Else, prefer 'files' items with link and 'type'=='video/mp4'
      3) Pick highest quality by 'height' or 'width' if available
    """
    candidates = []

    # 1) download[]
    for d in (video_json.get("download") or []):
        link = d.get("link")
        mime = (d.get("type") or "").lower()
        height = d.get("height") or 0
        width = d.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime):
            candidates.append(("download", height or width, link))

    # 2) files[]
    for f in (video_json.get("files") or []):
        link = f.get("link")
        mime = (f.get("type") or "").lower()
        height = f.get("height") or 0
        width = f.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime):
            candidates.append(("files", height or width, link))

    if not candidates:
        raise RuntimeError("No progressive MP4 candidates found in Vimeo response (download/files missing or HLS-only).")

    # Highest resolution first
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][2]  # return link

def stream_download(url: str, dest_path: str, chunk_bytes: int = 5 * 1024 * 1024):
    with requests.get(url, stream=True, timeout=60) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Download failed {r.status_code}: {r.text[:200]}")
        total = int(r.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_bytes):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
    size = os.path.getsize(dest_path)
    if total and size != total:
        # Not all servers set a Content-Length; if present and mismatched, flag it.
        raise RuntimeError(f"Incomplete download: expected {total} bytes, got {size} bytes")
    return size

# -------- RunPod handler --------

def handler(event):
    """
    Input:
      {
        "input": {
          "job_id": "20250911-abc123-video42",
          "vimeo_id": "123456789"
        }
      }
    Output:
      {
        "ok": true,
        "path": "/storage/{job_id}/raw/full.mp4",
        "bytes": 12345,
        "source_url": "https://...mp4"
      }
    """
    ensure_env()

    payload = event.get("input") or {}
    job_id = payload.get("job_id")
    vimeo_id = payload.get("vimeo_id") or payload.get("video_id")

    if not job_id:
        raise RuntimeError("Missing 'job_id' in input.")
    if not vimeo_id:
        raise RuntimeError("Missing 'vimeo_id' (or 'video_id') in input.")

    # Prepare paths
    raw_dir = f"/storage/{job_id}/raw"
    mkdir_p(raw_dir)
    out_path = f"{raw_dir}/full.mp4"

    # Query Vimeo for direct MP4
    # Limit fields to keep payload small/fast
    fields = "download.link,download.type,download.height,download.width,files.link,files.type,files.height,files.width,name,uri"
    vj = vimeo_get(f"/videos/{vimeo_id}", fields=fields)

    mp4_url = choose_best_mp4(vj)

    # Download to /storage/{job_id}/raw/full.mp4
    size = stream_download(mp4_url, out_path)

    # Minimal job metadata (you can expand later)
    meta_dir = f"/storage/{job_id}/metadata"
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

    return {
        "ok": True,
        "path": out_path,
        "bytes": size,
        "source_url": mp4_url
    }

# Start the serverless handler
runpod.serverless.start({"handler": handler})
