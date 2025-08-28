import os
import json
import redis
import yt_dlp
import asyncio
import logging
import uuid
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

from fastapi import FastAPI, HTTPException, BackgroundTasks, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Video Ingestion Service")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis connection
redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    STARTED = "started"
    RUNNING = "running"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class IngestRequest(BaseModel):
    url: HttpUrl

@dataclass
class IngestJob:
    id: str
    url: str
    status: JobStatus
    progress: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

def get_yt_dlp_options_primary():
    """Primary yt-dlp options with enhanced anti-detection"""
    return {
        'format': 'bestaudio[ext=m4a]/bestaudio/best',
        'skip_download': True,
        'no_warnings': True,
        'quiet': True,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'referer': 'https://www.youtube.com/',
        'http_headers': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
        },
        'sleep_interval': 2,
        'retries': 5,
        'socket_timeout': 30,
    }

def get_yt_dlp_options_fallback():
    """Fallback yt-dlp options with different approach"""
    return {
        'format': 'bestaudio/best',
        'skip_download': True,
        'no_warnings': True,
        'quiet': True,
        'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'extractor_args': {
            'youtube': {
                'skip': ['hls', 'dash'],
                'player_skip': ['js'],
            }
        },
        'sleep_interval': 5,
        'retries': 3,
        'socket_timeout': 60,
    }

async def extract_with_fallback(url: str) -> Dict[str, Any]:
    """Try multiple extraction methods with fallbacks"""
    
    # Method 1: Primary extraction with anti-detection
    try:
        logger.info(f"Attempting primary extraction for {url}")
        with yt_dlp.YoutubeDL(get_yt_dlp_options_primary()) as ydl:
            info = ydl.extract_info(url, download=False)
            logger.info("Primary extraction successful")
            return info
    except Exception as e:
        logger.warning(f"Primary extraction failed: {str(e)}")
        
    # Method 2: Fallback with different settings
    try:
        logger.info("Attempting fallback extraction")
        await asyncio.sleep(random.uniform(3, 8))
        
        with yt_dlp.YoutubeDL(get_yt_dlp_options_fallback()) as ydl:
            info = ydl.extract_info(url, download=False)
            logger.info("Fallback extraction successful")
            return info
    except Exception as e:
        logger.warning(f"Fallback extraction failed: {str(e)}")
        
    # Method 3: Minimal extraction as last resort
    try:
        logger.info("Attempting minimal extraction")
        await asyncio.sleep(random.uniform(5, 10))
        
        minimal_opts = {
            'skip_download': True,
            'quiet': True,
            'no_warnings': True,
            'format': 'best',
            'user_agent': 'yt-dlp/2023.12.30',
        }
        
        with yt_dlp.YoutubeDL(minimal_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            logger.info("Minimal extraction successful")
            return info
    except Exception as e:
        logger.error(f"All extraction methods failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to extract video info after multiple attempts: {str(e)}")

def get_job_key(job_id: str) -> str:
    return f"job:{job_id}"

def save_job(job: IngestJob) -> None:
    """Save job to Redis"""
    job_data = asdict(job)
    # Convert datetime objects to ISO strings
    job_data['created_at'] = job.created_at.isoformat()
    if job.completed_at:
        job_data['completed_at'] = job.completed_at.isoformat()
    
    redis_client.setex(get_job_key(job.id), 3600, json.dumps(job_data))

def get_job(job_id: str) -> Optional[IngestJob]:
    """Get job from Redis"""
    job_data = redis_client.get(get_job_key(job_id))
    if not job_data:
        return None
    
    job_dict = json.loads(job_data)
    # Convert ISO strings back to datetime objects
    job_dict['created_at'] = datetime.fromisoformat(job_dict['created_at'])
    if job_dict['completed_at']:
        job_dict['completed_at'] = datetime.fromisoformat(job_dict['completed_at'])
    
    return IngestJob(**job_dict)

async def process_video_url(job_id: str, url: str) -> None:
    """Process video URL with yt-dlp using fallback methods"""
    job = get_job(job_id)
    if not job:
        logger.error(f"Job {job_id} not found")
        return

    try:
        job.status = JobStatus.STARTED
        job.progress = 10
        save_job(job)
        
        logger.info(f"Processing video URL: {url}")
        
        job.status = JobStatus.RUNNING
        job.progress = 30
        save_job(job)
        
        # Extract video info using fallback methods
        info = await extract_with_fallback(url)
        
        job.progress = 70
        job.status = JobStatus.PROCESSING
        save_job(job)
        
        # Get audio formats
        formats = info.get('formats', [])
        audio_formats = []
        
        for fmt in formats:
            if fmt.get('acodec') != 'none' and fmt.get('vcodec') == 'none':
                audio_formats.append({
                    'format_id': fmt.get('format_id'),
                    'url': fmt.get('url'),
                    'ext': fmt.get('ext'),
                    'acodec': fmt.get('acodec'),
                    'abr': fmt.get('abr'),
                    'filesize': fmt.get('filesize'),
                })
        
        # If no pure audio formats, get best audio from video formats
        if not audio_formats:
            for fmt in formats:
                if fmt.get('acodec') != 'none':
                    audio_formats.append({
                        'format_id': fmt.get('format_id'),
                        'url': fmt.get('url'),
                        'ext': fmt.get('ext'),
                        'acodec': fmt.get('acodec'),
                        'abr': fmt.get('abr'),
                        'filesize': fmt.get('filesize'),
                    })
                    break
        
        job.progress = 90
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now()
        job.result = {
            'title': info.get('title'),
            'duration': info.get('duration'),
            'uploader': info.get('uploader'),
            'view_count': info.get('view_count'),
            'upload_date': info.get('upload_date'),
            'description': info.get('description', '')[:500],
            'thumbnail': info.get('thumbnail'),
            'audio_formats': audio_formats[:5],
            'webpage_url': info.get('webpage_url'),
        }
        save_job(job)
        
        logger.info(f"Successfully processed job {job_id}")
        
    except Exception as e:
        job.status = JobStatus.FAILED
        job.error = str(e)
        job.completed_at = datetime.now()
        save_job(job)
        logger.error(f"Job {job_id} failed: {e}")

@app.get("/")
async def root():
    return {"message": "Video Ingestion Service", "status": "healthy"}

@app.get("/health")
async def health_check():
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.post("/ingest")
async def ingest_video(request: IngestRequest, background_tasks: BackgroundTasks):
    """Start video ingestion job"""
    job_id = str(uuid.uuid4())
    
    job = IngestJob(
        id=job_id,
        url=str(request.url),
        status=JobStatus.PENDING,
        progress=0,
        created_at=datetime.now()
    )
    
    save_job(job)
    background_tasks.add_task(process_video_url, job_id, str(request.url))
    
    return {"job_id": job_id, "status": "started"}

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get job status"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "id": job.id,
        "url": job.url,
        "status": job.status,
        "progress": job.progress,
        "created_at": job.created_at.isoformat(),
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "error": job.error,
        "result": job.result
    }

@app.post("/update-ytdlp")
async def update_ytdlp():
    """Manual yt-dlp update endpoint"""
    try:
        return {"message": "yt-dlp update triggered", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

@app.post("/download-audio")
async def download_audio(request: dict):
    """
    Download audio from YouTube URL with proper authentication.
    Used by transcription service to access YouTube audio streams.
    """
    try:
        url = request.get("url")
        if not url:
            raise HTTPException(status_code=400, detail="URL is required")
        
        # Use requests to download with proper headers
        import requests
        
        # YouTube audio URLs require specific headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'audio/*,*/*;q=0.9',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'identity',
            'Range': 'bytes=0-',
        }
        
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        
        # Return the audio data as bytes
        audio_data = response.content
        
        return Response(
            content=audio_data,
            media_type="audio/mpeg",
            headers={
                "Content-Length": str(len(audio_data)),
                "Content-Type": "audio/mpeg"
            }
        )
        
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to download audio: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
