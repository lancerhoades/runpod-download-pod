import os
import json
import time
import pathlib
import requests
import runpod

VIMEO_API = "https://api.vimeo.com"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# --- Storage root detection (Serverless => /runpod-volume) ---
def resolve_storage_root() -> str:
    # Preferred for Serverless
    if os.path.isdir("/runpod-volume"):
        return "/runpod-volume"
    # Legacy fallbacks (some templates mount differently)
    if os.path.isdir("/runpod-volume"):
        return "/runpod-volume"
    if os.path.isdir("/runpod-volume"):
        return "/runpod-volume"
    raise RuntimeError(
        "No network volume found. On Serverless it should be at /runpod-volume. "
        "Attach a Network Volume in Endpoint > Advanced > Network Volume."
    )

STORAGE_ROOT = resolve_storage_root()

# -------- Helpers --------
def ensure_env():
    if not VIMEO_ACCESS_TOKEN:
        raise RuntimeError("VIMEO_ACCESS_TOKEN env var is required (set it in your RunPod template).")
    if not os.path.isdir(STORAGE_ROOT):
        raise RuntimeError(f"Storage root not available: {STORAGE_ROOT}")

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
    Prefer progressive MP4:
      1) 'download' list
      2) 'files' list
      Highest resolution first.
    """
    candidates = []

    for d in (video_json.get("download") or []):
        link = d.get("link")
        mime = (d.get("type") or "").lower()
        height = d.get("height") or 0
        width = d.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime):
            candidates.append(("download", height or width, link))

    for f in (video_json.get("files") or []):
        link = f.get("link")
        mime = (f.get("type") or "").lower()
        height = f.get("height") or 0
        width = f.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime):
            candidates.append(("files", height or width, link))

    if not candidates:
        raise RuntimeError("No progressive MP4 candidates found in Vimeo response.")

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][2]

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
        raise RuntimeError(f"Incomplete download: expected {total} bytes, got {size} bytes")
    return size

# -------- RunPod handler --------
def handler(event):
    """
    Input:
      {
        "job_id": "2025....-abc123",
        "vimeo_id": "1114674380"
      }
    Output:
      {
        "ok": true,
        "path": "/runpod-volume/{job_id}/raw/full.mp4",
        "bytes": 12345,
        "source_url": "https://...mp4"
      }
    """
    ensure_env()
    payload = (event.get("input") or {}) if isinstance(event, dict) else {}
    job_id = payload.get("job_id")
    vimeo_id = payload.get("vimeo_id") or payload.get("video_id")

    if not job_id:
        raise RuntimeError("Missing 'job_id' in input.")
    if not vimeo_id:
        raise RuntimeError("Missing 'vimeo_id' (or 'video_id') in input.")

    raw_dir = f"{STORAGE_ROOT}/{job_id}/raw"
    mkdir_p(raw_dir)
    out_path = f"{raw_dir}/full.mp4"

    fields = "download.link,download.type,download.height,download.width,files.link,files.type,files.height,files.width,name,uri"
    vj = vimeo_get(f"/videos/{vimeo_id}", fields=fields)
    mp4_url = choose_best_mp4(vj)
    size = stream_download(mp4_url, out_path)

    meta_dir = f"{STORAGE_ROOT}/{job_id}/metadata"
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
