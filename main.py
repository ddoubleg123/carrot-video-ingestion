import os
import json
import uuid
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl
import yt_dlp
import redis
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Video Ingestion Service", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for your domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis connection
redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class IngestJob:
    job_id: str
    url: str
    status: JobStatus
    progress: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

class IngestRequest(BaseModel):
    url: HttpUrl
    
class IngestResponse(BaseModel):
    job_id: str
    status: str
    message: str

class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    progress: int
    created_at: str
    completed_at: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

# yt-dlp configuration
YDL_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "socket_timeout": 15,
    "retries": 3,
    "fragment_retries": 2,
    "skip_download": True,
    "writesubtitles": True,
    "writeautomaticsub": True,
    "subtitleslangs": ["en"],
    "format": "bestaudio/best",
    "extractaudio": True,
    "audioformat": "mp3",
    "outtmpl": "/tmp/%(id)s.%(ext)s",
}

def get_job_key(job_id: str) -> str:
    return f"job:{job_id}"

def save_job(job: IngestJob) -> None:
    """Save job to Redis"""
    key = get_job_key(job.job_id)
    data = asdict(job)
    # Convert datetime objects to ISO strings
    data["created_at"] = job.created_at.isoformat()
    if job.completed_at:
        data["completed_at"] = job.completed_at.isoformat()
    redis_client.setex(key, 3600, json.dumps(data))  # 1 hour TTL

def get_job(job_id: str) -> Optional[IngestJob]:
    """Get job from Redis"""
    key = get_job_key(job_id)
    data = redis_client.get(key)
    if not data:
        return None
    
    job_data = json.loads(data)
    # Convert ISO strings back to datetime objects
    job_data["created_at"] = datetime.fromisoformat(job_data["created_at"])
    if job_data["completed_at"]:
        job_data["completed_at"] = datetime.fromisoformat(job_data["completed_at"])
    
    return IngestJob(**job_data)

async def process_video_url(job_id: str, url: str) -> None:
    """Process video URL with yt-dlp"""
    job = get_job(job_id)
    if not job:
        return
    
    try:
        # Update status to processing
        job.status = JobStatus.PROCESSING
        job.progress = 10
        save_job(job)
        
        logger.info(f"Processing job {job_id} for URL: {url}")
        
        # Check cache first
        cache_key = f"video:{hash(url)}"
        cached_result = redis_client.get(cache_key)
        
        if cached_result:
            logger.info(f"Cache hit for URL: {url}")
            job.result = json.loads(cached_result)
            job.progress = 100
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now()
            save_job(job)
            return
        
        job.progress = 30
        save_job(job)
        
        # Extract video info
        with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
            info = ydl.extract_info(url, download=False)
            
        job.progress = 70
        save_job(job)
        
        # Process the extracted info
        result = {
            "video_id": info.get("id"),
            "title": info.get("title"),
            "description": info.get("description"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "upload_date": info.get("upload_date"),
            "view_count": info.get("view_count"),
            "thumbnail": info.get("thumbnail"),
            "formats": [],
            "subtitles": {},
            "automatic_captions": {}
        }
        
        # Extract audio formats
        for fmt in info.get("formats", []):
            if fmt.get("acodec") and fmt.get("acodec") != "none":
                result["formats"].append({
                    "format_id": fmt.get("format_id"),
                    "url": fmt.get("url"),
                    "ext": fmt.get("ext"),
                    "acodec": fmt.get("acodec"),
                    "filesize": fmt.get("filesize"),
                })
        
        # Extract subtitles
        result["subtitles"] = info.get("subtitles", {})
        result["automatic_captions"] = info.get("automatic_captions", {})
        
        # Cache the result for 24 hours
        redis_client.setex(cache_key, 86400, json.dumps(result))
        
        # Complete the job
        job.result = result
        job.progress = 100
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now()
        save_job(job)
        
        logger.info(f"Successfully processed job {job_id}")
        
    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        job.status = JobStatus.FAILED
        job.error = str(e)
        job.completed_at = datetime.now()
        save_job(job)

@app.get("/")
async def root():
    return {"message": "Video Ingestion Service", "status": "healthy"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test Redis connection
        redis_client.ping()
        
        # Test yt-dlp with a known stable video
        test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
            ydl.extract_info(test_url, download=False)
        
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.post("/ingest", response_model=IngestResponse)
async def ingest_video(request: IngestRequest, background_tasks: BackgroundTasks):
    """Start video ingestion job"""
    job_id = str(uuid.uuid4())
    url = str(request.url)
    
    # Create job
    job = IngestJob(
        job_id=job_id,
        url=url,
        status=JobStatus.QUEUED,
        progress=0,
        created_at=datetime.now()
    )
    
    save_job(job)
    
    # Start background processing
    background_tasks.add_task(process_video_url, job_id, url)
    
    logger.info(f"Created ingestion job {job_id} for URL: {url}")
    
    return IngestResponse(
        job_id=job_id,
        status=job.status,
        message="Ingestion job started"
    )

@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get job status and result"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        created_at=job.created_at.isoformat(),
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        error=job.error,
        result=job.result
    )

@app.post("/update-ytdlp")
async def update_ytdlp():
    """Manual yt-dlp update endpoint"""
    try:
        # This would typically update the yt-dlp binary
        # For now, just return success
        return {"message": "yt-dlp update triggered", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
