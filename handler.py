import os, json, time, pathlib, subprocess, requests, runpod, boto3
from botocore.client import Config

VOLUME_PATH = os.getenv("RUNPOD_VOLUME_PATH", "/runpod-volume")
VIMEO_API   = "https://api.vimeo.com"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")  # required for S3 upload
S3_PREFIX     = os.getenv("S3_PREFIX_BASE", "jobs")

MAX_HEIGHT = int(os.getenv("MAX_HEIGHT", "1080"))
PRINT_EVERY_MB = int(os.getenv("PRINT_EVERY_MB", "100"))

s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(s3={"addressing_style":"virtual"}))

def ensure_env():
    if not VIMEO_ACCESS_TOKEN:
        raise RuntimeError("VIMEO_ACCESS_TOKEN is required.")
    if not os.path.isdir(VOLUME_PATH):
        raise RuntimeError(f"RunPod network volume missing at {VOLUME_PATH}. Attach a Network Volume to the endpoint.")
    if not AWS_S3_BUCKET:
        raise RuntimeError("AWS_S3_BUCKET is required to upload audio for transcription.")

def mkdir_p(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def vimeo_get(path, params=None, fields=None):
    headers = {"Authorization": f"bearer {VIMEO_ACCESS_TOKEN}",
               "Accept": "application/vnd.vimeo.*+json;version=3.4"}
    params = params or {}
    if fields: params["fields"] = fields
    r = requests.get(f"{VIMEO_API}{path}", headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Vimeo GET {r.status_code}: {r.text[:300]}")
    return r.json()

def choose_best_mp4(vj):
    cand = []
    for d in (vj.get("download") or []):
        link = d.get("link"); mime = (d.get("type") or "").lower()
        h = d.get("height") or 0; w = d.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime): cand.append((h or w, h, link))
    for f in (vj.get("files") or []):
        link = f.get("link"); mime = (f.get("type") or "").lower()
        h = f.get("height") or 0; w = f.get("width") or 0
        if link and ("mp4" in mime or mime == "" or "video/" in mime): cand.append((h or w, h, link))
    if not cand: raise RuntimeError("No progressive MP4 found.")
    cand.sort(key=lambda x: x[0], reverse=True)
    under = [c for c in cand if c[1] and c[1] <= MAX_HEIGHT]
    return (under or cand)[0][2]

def stream_download(url, dst, chunk=4*1024*1024):
    with requests.get(url, stream=True, timeout=(30, 600)) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Download failed {r.status_code}: {r.text[:300]}")
        downloaded = 0; next_report = PRINT_EVERY_MB*1024*1024
        with open(dst,"wb") as f:
            for ch in r.iter_content(chunk_size=chunk):
                if not ch: continue
                f.write(ch); downloaded += len(ch)
                if downloaded >= next_report:
                    print(f"[DL] {downloaded//(1024*1024)} MB...", flush=True)
                    next_report += PRINT_EVERY_MB*1024*1024
    return os.path.getsize(dst)

def make_audio(src_mp4, dst_mp3):
    # 16 kHz mono 64 kbps MP3 (tiny but good for ASR)
    cmd = ["ffmpeg","-hide_banner","-loglevel","error","-y",
           "-i", src_mp4, "-vn", "-ac", "1", "-ar", "16000", "-b:a", "64k", dst_mp3]
    subprocess.run(cmd, check=True)
    return os.path.getsize(dst_mp3)

def s3_key(job_id, name): return f"{S3_PREFIX}/{job_id}/inputs/{int(time.time())}-{name}"
def s3_upload_and_presign(local, job_id):
    key = s3_key(job_id, os.path.basename(local))
    s3.upload_file(local, AWS_S3_BUCKET, key)
    url = s3.generate_presigned_url("get_object",
           Params={"Bucket": AWS_S3_BUCKET, "Key": key}, ExpiresIn=3600)
    return key, url

def handler(event):
    ensure_env()
    payload = (event.get("input") or {}) if isinstance(event, dict) else {}
    job_id = payload.get("job_id"); vimeo_id = payload.get("vimeo_id") or payload.get("video_id")
    if not job_id:  raise RuntimeError("job_id required.")
    if not vimeo_id:raise RuntimeError("vimeo_id (or video_id) required.")

    raw_dir = f"{VOLUME_PATH}/{job_id}/raw"; mkdir_p(raw_dir)
    meta_dir = f"{VOLUME_PATH}/{job_id}/metadata"; mkdir_p(meta_dir)
    mp4_path = f"{raw_dir}/full.mp4"
    mp3_path = f"{raw_dir}/audio_16k_mono64k.mp3"

    print(f"[INFO] job={job_id} vimeo={vimeo_id}", flush=True)
    fields = "download.link,download.type,download.height,download.width,files.link,files.type,files.height,files.width,name,uri"
    vj = vimeo_get(f"/videos/{vimeo_id}", fields=fields)
    src = choose_best_mp4(vj)
    print(f"[INFO] MP4: {src}", flush=True)
    bytes_mp4 = stream_download(src, mp4_path)
    print(f"[INFO] MP4 done: {bytes_mp4} bytes", flush=True)

    bytes_mp3 = make_audio(mp4_path, mp3_path)
    print(f"[INFO] MP3 done: {bytes_mp3} bytes", flush=True)

    key_mp3, audio_url = s3_upload_and_presign(mp3_path, job_id)
    print(f"[INFO] S3 audio: s3://{AWS_S3_BUCKET}/{key_mp3}", flush=True)

    with open(f"{meta_dir}/job.json","w",encoding="utf-8") as f:
        json.dump({
          "job_id": job_id, "vimeo_id": vimeo_id, "downloaded_at_utc": int(time.time()),
          "raw_full_path": mp4_path, "bytes": bytes_mp4, "audio_mp3_path": mp3_path,
          "audio_bytes": bytes_mp3, "audio_s3_bucket": AWS_S3_BUCKET, "audio_s3_key": key_mp3,
          "audio_url": audio_url, "source_url": src, "video_uri": vj.get("uri"), "name": vj.get("name")
        }, f, ensure_ascii=False, indent=2)

    return {"ok": True,
            "path": mp4_path,
            "bytes": bytes_mp4,
            "audio_path": mp3_path,
            "audio_bytes": bytes_mp3,
            "audio_s3_bucket": AWS_S3_BUCKET,
            "audio_s3_key": key_mp3,
            "audio_url": audio_url,
            "source_url": src}
runpod.serverless.start({"handler": handler})
