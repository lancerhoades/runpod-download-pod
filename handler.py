import os, io, sys, json, time, uuid, tempfile, logging, mimetypes, re, math
import runpod
import boto3
from botocore.client import Config
import requests
from yt_dlp import YoutubeDL

# -------- ENV --------
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET      = os.getenv("AWS_S3_BUCKET")
S3_PREFIX_BASE     = os.getenv("S3_PREFIX_BASE", "jobs")
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")  # required for private/managed Vimeo

# Resume / logging tunables
LOG_LEVEL             = os.getenv("LOG_LEVEL","INFO").upper()
DOWNLOAD_CHUNK_MB     = int(os.getenv("DOWNLOAD_CHUNK_MB", "4"))       # per read
DOWNLOAD_MAX_RETRIES  = int(os.getenv("DOWNLOAD_MAX_RETRIES","6"))     # retry attempts
DOWNLOAD_PROGRESS_SEC = float(os.getenv("DOWNLOAD_PROGRESS_SEC","5"))  # progress log cadence

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET env is required")

logging.basicConfig(level=getattr(logging, LOG_LEVEL, 20), format="%(asctime)s | %(levelname)s | %(message)s")
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

# ---------- Vimeo helpers ----------
_VIMEO_ID_RX = re.compile(r"(?:vimeo\.com/(?:video/|manage/videos/)?)(\d+)")

def _vimeo_id_from(url_or_id: str|int|None):
    if url_or_id is None:
        return None
    s = str(url_or_id)
    m = _VIMEO_ID_RX.search(s)
    if m: return m.group(1)
    return s if s.isdigit() else None

def _vimeo_get_best_file_url(vimeo_id: str) -> tuple[str,str,dict]:
    """
    Returns (download_url, suggested_filename, headers_for_get).
    Prefers progressive mp4 (highest width), falls back to first file.
    """
    if not VIMEO_ACCESS_TOKEN:
        raise RuntimeError("VIMEO_ACCESS_TOKEN not set; cannot fetch private Vimeo files.")

    api = f"https://api.vimeo.com/videos/{vimeo_id}"
    params = {"fields": "name,files,download"}
    headers = {"Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}"}
    r = requests.get(api, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Vimeo API {r.status_code}: {r.text[:500]}")
    data = r.json()

    files = data.get("files") or []
    best = None
    best_w = -1
    for f in files:
        link = f.get("link")
        if not link:
            continue
        if f.get("type") == "video/mp4" or (f.get("mime_type") or "").startswith("video/"):
            w = f.get("width") or 0
            if w > best_w:
                best_w = w
                best = f
    if not best:
        for d in (data.get("download") or []):
            link = d.get("link")
            if link:
                best = d
                break
    if not best:
        raise RuntimeError("No downloadable file found via Vimeo API.")

    link = best.get("link")
    name = (data.get("name") or f"vimeo-{vimeo_id}") + ".mp4"
    # Some Vimeo file links require same Authorization header to GET the binary
    get_headers = {"Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}"}
    return link, name, get_headers

# ---------- Resumable downloader ----------
def _head_for_size_etag(url: str, headers: dict|None) -> tuple[int|None, str|None, bool]:
    """Return (content_length, etag, supports_range)."""
    try:
        r = requests.head(url, headers=headers or {}, timeout=30, allow_redirects=True)
        cl = int(r.headers.get("Content-Length")) if r.headers.get("Content-Length") else None
        et = r.headers.get("ETag")
        ar = "bytes" in (r.headers.get("Accept-Ranges","").lower())
        return cl, et, ar
    except Exception:
        return None, None, False

def _resumable_download(url: str, dst: str, headers: dict|None=None):
    """
    Download with resume support:
      - Use HEAD to discover length and Accept-Ranges
      - If dst exists, continue from current size with Range
      - Retries with exponential backoff
      - Periodic progress logs (every DOWNLOAD_PROGRESS_SEC)
    """
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    part = dst + ".part"
    pos = os.path.getsize(part) if os.path.exists(part) else 0

    total, etag, can_range = _head_for_size_etag(url, headers)
    if total:
        log.info(f"Remote size: {total/1e6:.1f} MB | Accept-Ranges={can_range} | ETag={etag}")

    chunk = DOWNLOAD_CHUNK_MB * (1<<20)
    attempts = 0
    backoff = 1.0
    t0 = time.time()
    last_log = 0.0

    while True:
        try:
            rng_headers = dict(headers or {})
            if can_range and pos > 0:
                rng_headers["Range"] = f"bytes={pos}-"
            with requests.get(url, headers=rng_headers, stream=True, timeout=None, allow_redirects=True) as r:
                if (can_range and pos > 0 and r.status_code not in (206, 200)) or (not can_range and r.status_code not in (200,)):
                    raise RuntimeError(f"HTTP {r.status_code} for ranged GET")
                mode = "ab" if (can_range and pos > 0) else "wb"
                with open(part, mode) as f:
                    for chunk_iter in r.iter_content(chunk_size=chunk):
                        if not chunk_iter:
                            continue
                        f.write(chunk_iter)
                        pos += len(chunk_iter)
                        now = time.time()
                        if now - last_log >= DOWNLOAD_PROGRESS_SEC:
                            rate = (pos / max(1e-9, now - t0)) / 1e6
                            if total:
                                pct = (pos / total) * 100.0
                                log.info(f"Downloading… {pos/1e6:.1f}/{total/1e6:.1f} MB ({pct:.1f}%) ~{rate:.1f} MB/s")
                            else:
                                log.info(f"Downloading… {pos/1e6:.1f} MB ~{rate:.1f} MB/s")
                            last_log = now
            # Completed
            os.replace(part, dst)
            # final log
            elapsed = max(1e-9, time.time() - t0)
            rate = (pos / elapsed) / 1e6
            if total:
                log.info(f"Done: {pos/1e6:.1f}/{total/1e6:.1f} MB in {elapsed:.1f}s (~{rate:.1f} MB/s)")
            else:
                log.info(f"Done: {pos/1e6:.1f} MB in {elapsed:.1f}s (~{rate:.1f} MB/s)")
            return
        except Exception as e:
            attempts += 1
            if attempts > DOWNLOAD_MAX_RETRIES:
                raise RuntimeError(f"Download failed after {DOWNLOAD_MAX_RETRIES} retries: {e}")
            log.warning(f"Download error ({e}); retry {attempts}/{DOWNLOAD_MAX_RETRIES} after {backoff:.1f}s, will resume at {pos/1e6:.1f} MB")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

def _download_direct_resumable(source_url: str, headers=None) -> str:
    tmpdir = tempfile.mkdtemp(prefix="dl_")
    ext = _pick_ext(source_url)
    dst = os.path.join(tmpdir, f"download-{uuid.uuid4().hex[:6]}.{ext}")
    _resumable_download(source_url, dst, headers=headers or {})
    return dst

# ---------- yt-dlp helper for non-Vimeo ----------
def _download_with_ytdlp(source: str, prefer_audio=True, headers=None) -> tuple[str,str]:
    tmpdir = tempfile.mkdtemp(prefix="dl_")
    base_out = os.path.join(tmpdir, "%(title).200B-%(id)s.%(ext)s")
    ydl_opts_common = {
        "outtmpl": base_out,
        "quiet": False if LOG_LEVEL=="DEBUG" else True,
        "noprogress": False if LOG_LEVEL=="DEBUG" else True,
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

def _looks_like_streaming_site(url: str) -> bool:
    return any(p in url.lower() for p in (
        "youtube.com","youtu.be","vimeo.com","tiktok.com","facebook.com","x.com","twitter.com"
    ))

# ---------- Handler ----------
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
        "s3_audio_url": "https://<presigned>",
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

    # Vimeo first: use API with access token (more reliable for private/manage links)
    vid = _vimeo_id_from(source_url) or _vimeo_id_from(vimeo_id)
    if vid:
        try:
            link, suggested_name, get_headers = _vimeo_get_best_file_url(vid)
            log.info(f"[{job_id}] Vimeo API resolved file: {suggested_name}")
            local_path = _download_direct_resumable(link, headers=get_headers)
            suggested = suggested_name
        except Exception as e:
            log.warning(f"[{job_id}] Vimeo API path failed ({e}); trying yt-dlp fallback.")
            headers = {"Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}"} if VIMEO_ACCESS_TOKEN else {}
            local_path, suggested = _download_with_ytdlp(source_url, prefer_audio=True, headers=headers)
    else:
        # Non-Vimeo
        if _looks_like_streaming_site(source_url):
            local_path, suggested = _download_with_ytdlp(source_url, prefer_audio=True, headers={})
        else:
            local_path = _download_direct_resumable(source_url)
            suggested = os.path.basename(local_path)

    key = _s3_key(job_id, "inputs", suggested)
    log.info(f"[{job_id}] uploading to s3://{AWS_S3_BUCKET}/{key}")
    s3.upload_file(local_path, AWS_S3_BUCKET, key)
    url = _presign(key)

    out = {
        "job_id": job_id,
        "s3_uri": f"s3://{AWS_S3_BUCKET}/{key}",
        "input_key": key,
        "s3_audio_url": url,
        "audio_url": url,
        "console": _console_link(job_id)
    }
    log.info(f"[{job_id}] done: {json.dumps(out)[:300]}")
    return out

runpod.serverless.start({"handler": handle})
