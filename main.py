import os
import re
import time
import glob
import uuid
import asyncio
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import yt_dlp

app = FastAPI(title="FastToMP High-Speed Media Worker")

# Allow all CORS origins for seamless cross-domain requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DOWNLOAD_DIR = "/tmp/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# In-memory job store
jobs = {}
executor = ThreadPoolExecutor(max_workers=4)

def sanitize_filename(title: str, ext: str = "") -> str:
    cleaned = re.sub(r'[\/:*?"<>|#\'`~,;=!@$%^&(){}\[\]]', '_', title)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'_+', '_', cleaned).strip(" ._-\t\n\r")
    if not cleaned:
        cleaned = "YouTube_Media"
    if ext:
        ext = ext.lstrip(".")
        cleaned = f"{cleaned}.{ext}"
    return cleaned

def cleanup_old_files():
    now = time.time()
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*")):
        try:
            if os.path.isfile(f) and (now - os.path.getmtime(f)) > 1800:
                os.remove(f)
        except Exception:
            pass

class AnalyzeRequest(BaseModel):
    url: str

class ConvertRequest(BaseModel):
    id: str
    type: str = "video"  # "video" or "audio"
    quality: Optional[str] = "720p"
    bitrate: Optional[str] = "320k"
    title: Optional[str] = "YouTube_Media"

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "fasttomp-render-worker",
        "uptime": time.time(),
        "active_jobs": len(jobs)
    }

@app.post("/analyze")
def analyze(req: AnalyzeRequest):
    cleanup_old_files()
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL")

    ydl_opts = {
        'skip_download': True,
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web']
            }
        },
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
        }
    }

    proxy = os.environ.get("YOUTUBE_PROXY")
    if proxy:
        ydl_opts['proxy'] = proxy

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        video_id = info.get("id")
        title = info.get("title", "YouTube Video")
        author = info.get("uploader") or info.get("channel") or "YouTube Creator"
        thumbnail = info.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
        duration = info.get("duration_string") or str(info.get("duration", "3:00"))

        formats = info.get("formats", [])
        available_heights = set()
        for f in formats:
            h = f.get("height")
            vc = f.get("vcodec", "none")
            if h and vc != "none":
                available_heights.add(h)

        target_heights = [1080, 720, 480, 360]
        video_formats = []
        for th in target_heights:
            has_height = any(h >= th for h in available_heights)
            if has_height or th <= 720:
                label_map = {
                    1080: "1080p Full HD",
                    720: "720p HD",
                    480: "480p Standard",
                    360: "360p Medium"
                }
                video_formats.append({
                    "quality": f"{th}p",
                    "label": label_map.get(th, f"{th}p"),
                    "ext": "mp4",
                    "filesize": "HD Quality" if th >= 720 else "Standard",
                    "download_url": "",
                    "has_audio": True
                })

        audio_presets = [
            {"bitrate": "320k", "label": "320 kbps (High Quality MP3)", "ext": "mp3", "download_url": "", "filesize": "320 kbps HQ"},
            {"bitrate": "192k", "label": "192 kbps (Standard MP3)", "ext": "mp3", "download_url": "", "filesize": "192 kbps"},
            {"bitrate": "128k", "label": "128 kbps (Fast MP3)", "ext": "mp3", "download_url": "", "filesize": "128 kbps Fast"}
        ]

        return {
            "success": True,
            "data": {
                "id": video_id,
                "title": title,
                "author": author,
                "thumbnail": thumbnail,
                "duration": duration,
                "video_formats": video_formats,
                "audio_formats": audio_presets
            }
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not extract video details: {str(e)}")

def run_conversion_task(job_id: str, video_id: str, req_type: str, quality: str, bitrate: str, title: str):
    token = f"dl_{uuid.uuid4().hex}"
    canonical_url = f"https://www.youtube.com/watch?v={video_id}"

    def progress_hook(d):
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 1
            downloaded = d.get('downloaded_bytes', 0)
            percent = int((downloaded / total) * 75)
            if job_id in jobs:
                jobs[job_id]["progress"] = max(15, min(90, 15 + percent))
                jobs[job_id]["message"] = f"Downloading stream ({jobs[job_id]['progress']}%)"

    try:
        jobs[job_id]["progress"] = 20
        jobs[job_id]["message"] = "Processing video stream..."

        common_ydl_opts = {
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'web']
                }
            },
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
            },
            'progress_hooks': [progress_hook],
            'quiet': True,
            'no_warnings': True,
        }

        proxy = os.environ.get("YOUTUBE_PROXY")
        if proxy:
            common_ydl_opts['proxy'] = proxy

        if req_type == "audio":
            file_name = sanitize_filename(title, "mp3")
            out_file = os.path.join(DOWNLOAD_DIR, f"{token}.mp3")
            quality_map = {"320k": "0", "192k": "2", "128k": "4"}
            audio_q = quality_map.get(bitrate, "2")

            ydl_opts = {
                **common_ydl_opts,
                'format': 'bestaudio/best',
                'outtmpl': os.path.join(DOWNLOAD_DIR, f"{token}.%(ext)s"),
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': audio_q,
                }],
            }
        else:
            height = int(re.sub(r'[^0-9]', '', quality) or "720")
            file_name = sanitize_filename(f"{title}_{height}p", "mp4")
            out_file = os.path.join(DOWNLOAD_DIR, f"{token}.mp4")

            format_spec = f"bestvideo[height<={height}][vcodec^=avc1]+bestaudio[acodec^=mp4a]/bestvideo[height<={height}]+bestaudio/best[height<={height}]/best"
            ydl_opts = {
                **common_ydl_opts,
                'format': format_spec,
                'outtmpl': out_file,
                'merge_output_format': 'mp4',
            }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([canonical_url])

        if os.path.exists(out_file) and os.path.getsize(out_file) > 1000:
            jobs[job_id].update({
                "status": "completed",
                "progress": 100,
                "download_token": token,
                "download_url": f"/download/{token}/{file_name}",
                "filename": file_name,
                "filepath": out_file,
                "message": "Ready for download!"
            })
        else:
            jobs[job_id].update({
                "status": "failed",
                "message": "Conversion completed but output file is missing or empty."
            })
    except Exception as e:
        jobs[job_id].update({
            "status": "failed",
            "message": f"Conversion error: {str(e)}"
        })

@app.post("/convert")
def convert(req: ConvertRequest):
    cleanup_old_files()
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    jobs[job_id] = {
        "id": job_id,
        "status": "processing",
        "progress": 10,
        "message": "Queuing conversion job...",
        "created_at": time.time()
    }

    executor.submit(
        run_conversion_task,
        job_id,
        req.id,
        req.type,
        req.quality or "720p",
        req.bitrate or "320k",
        req.title or "YouTube_Media"
    )

    return {
        "success": True,
        "job_id": job_id,
        "status": "processing",
        "progress": 10,
        "message": "Conversion job started successfully."
    }

@app.get("/job-status/{job_id}")
def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return {
        "success": job["status"] != "failed",
        "job_id": job["id"],
        "status": job["status"],
        "progress": job.get("progress", 0),
        "download_token": job.get("download_token"),
        "download_url": job.get("download_url"),
        "filename": job.get("filename"),
        "message": job.get("message", "")
    }

@app.get("/download/{token}/{filename}")
def download(token: str, filename: str):
    matches = glob.glob(os.path.join(DOWNLOAD_DIR, f"{token}.*"))
    if not matches:
        raise HTTPException(status_code=404, detail="Download file expired or not found")
    
    file_path = matches[0]
    media_type = "audio/mpeg" if file_path.endswith(".mp3") else "video/mp4"
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
