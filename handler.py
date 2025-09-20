import os, io, sys, json, time, uuid, tempfile, logging, mimetypes, re
import runpod
import boto3
from botocore.client import Config
import requests
from yt_dlp import YoutubeDL

# -------- ENV --------
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET    = os.getenv("AWS_S3_BUCKET")
S3_PREFIX_BASE   = os.getenv("S3_PREFIX_BASE", "jobs")
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")  # optional

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET env is required")

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("download-pod")

s3 = boto3.client("s3", region_name=AWS_REGION,
                  config=Config(s3={"addressing_style": "virtual"}))

def _s3_key(job_id: str, *parts: str) -> str:
    safe = [p.strip("/").replace("\\","/") for p in parts if p]
    return "/".join([S3_PREFIX_BASE.strip("/"), job_id] + safe)

def _presign(key: str, expires=7*24*3600) -> str:
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": AWS_S3_BUCKET, "Key": key},
                                     ExpiresIn=expires)

def _console_link(job_id: str) -> str:
    return (f"https://s3.console.aws.amazon.com/s3/buckets/{AWS_S3_BUCKET}"
            f"?region={AWS_REGION}&prefix={S3_PREFIX_BASE}/{job_id}/&showversions=false")

def _pick_ext(url_or_title: str, fallback="mp4") -> str:
    url_or_title = url_or_title.lower()
    for ext in ("m4a","mp3","mp4","mkv","webm","wav","aac","flac","mov"):
        if url_or_title.endswith("."+ext) or f".{ext}?" in url_or_title:
            return ext
    return fallback

def _download_with_ytdlp(source: str, prefer_audio=True, headers=None) -> tuple[str,str]:
    """
    Downloads the media to a temp file and returns (local_path, suggested_filename).
    prefer_audio=True tries bestaudio first (no postprocessing), then falls back to best (video).
    """
    tmpdir = tempfile.mkdtemp(prefix="dl_")
    base_out = os.path.join(tmpdir, "%(title).200B-%(id)s.%(ext)s")
    ydl_opts_common = {
        "outtmpl": base_out,
        "quiet": True,
        "noprogress": True,
        "retries": 5,
        "http_headers": headers or {},
        "postprocessors": []
    }

    def _run(fmt):
        opts = dict(ydl_opts_common)
        if fmt: opts["format"] = fmt
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(source, download=True)
            filename = ydl.prepare_filename(info)
            return filename, info

    if prefer_audio:
        try:
            fpath, info = _run("bestaudio/best")
            return fpath, os.path.basename(fpath)
        except Exception as e:
            log.warning(f"bestaudio failed: {e}; falling back to best")

    fpath, info = _run("best")
    return fpath, os.path.basename(fpath)

def _download_direct(source_url: str) -> str:
    tmpdir = tempfile.mkdtemp(prefix="dl_")
    ext = _pick_ext(source_url)
    dst = os.path.join(tmpdir, f"download-{uuid.uuid4().hex[:6]}.{ext}")
    with requests.get(source_url, stream=True, timeout=None) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk:
                    f.write(chunk)
    return dst

def _looks_like_streaming_site(url: str) -> bool:
    return any(p in url.lower() for p in (
        "youtube.com","youtu.be","vimeo.com","tiktok.com","facebook.com","x.com","twitter.com"
    ))

def handle(event):
    """
    Inputs:
      { "job_id": "...", "source_url": "https://...", "vimeo_id": "12345" }
    Returns:
      {
        "job_id": "...",
        "s3_uri": "s3://bucket/jobs/<job_id>/inputs/<filename>",
        "input_key": "jobs/<job_id>/inputs/<filename>",
        "audio_url": "https://<presigned>",
        "s3_audio_url": "https://<presigned>",   # alias for back-compat with main.py
        "console": "https://s3.console.aws.amazon.com/.../jobs/<job_id>/"
      }
    """
    payload = event.get("input") if isinstance(event, dict) else None
    if not isinstance(payload, dict):
        payload = event or {}

    job_id    = payload.get("job_id")
    source_url = payload.get("source_url")
    vimeo_id   = payload.get("vimeo_id") or payload.get("video_id")

    if not job_id:
        raise ValueError("job_id is required")
    if not source_url and vimeo_id:
        source_url = f"https://vimeo.com/{vimeo_id}"
    if not source_url:
        return {"error": "source_url or vimeo_id is required", "job_id": job_id}

    log.info(f"[{job_id}] downloading: {source_url}")

    headers = {}
    if VIMEO_ACCESS_TOKEN and "vimeo.com" in source_url.lower():
        headers["Authorization"] = f"Bearer {VIMEO_ACCESS_TOKEN}"

    try:
        if _looks_like_streaming_site(source_url):
            local_path, suggested = _download_with_ytdlp(source_url, prefer_audio=True, headers=headers)
        else:
            local_path = _download_direct(source_url)
            suggested = os.path.basename(local_path)
    except Exception as e:
        log.exception("Download failed")
        return {"error": f"download failed: {e}", "job_id": job_id}

    key = _s3_key(job_id, "inputs", suggested)
    log.info(f"[{job_id}] uploading to s3://{AWS_S3_BUCKET}/{key}")
    s3.upload_file(local_path, AWS_S3_BUCKET, key)
    url = _presign(key)

    out = {
        "job_id": job_id,
        "s3_uri": f"s3://{AWS_S3_BUCKET}/{key}",
        "input_key": key,
        "s3_audio_url": url,  # back-compat
        "audio_url": url,
        "console": _console_link(job_id)
    }
    log.info(f"[{job_id}] done: {json.dumps(out)[:300]}")
    return out

runpod.serverless.start({"handler": handle})
