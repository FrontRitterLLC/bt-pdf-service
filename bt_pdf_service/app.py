# SPDX-License-Identifier: AGPL-3.0-only
"""HTTP front for BabelDOC: PDF in, translated PDF out.

    POST   /jobs              multipart: file, lang_out, [lang_in], [dual], base_url, model, api_key
    GET    /jobs/{id}         state, progress, stage, queue position, error
    GET    /jobs/{id}/result  the translated PDF
    DELETE /jobs/{id}         cancel and delete
    GET    /                  service info + source code link (AGPL-3.0 section 13)

Each job runs BabelDOC in its own child process (worker.py). The user's file and API key are kept
only for the job's lifetime: the key lives in memory and is handed to the worker on stdin, and the
job folder is deleted when the result is fetched (after a short grace period), on DELETE, or after BT_PDF_JOB_TTL.
"""

import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


SOURCE_URL = env("BT_PDF_SOURCE_URL", "")  # public repo of this service; shown to every user (AGPL §13)
ORIGINS = [o.strip() for o in env("BT_PDF_ALLOWED_ORIGINS", "http://127.0.0.1:8850").split(",") if o.strip()]
# The server makes requests to base_url on the user's behalf, so only known LLM hosts (no SSRF into our network).
LLM_HOSTS = {
    h.strip()
    for h in env(
        "BT_PDF_LLM_HOSTS", "api.deepseek.com,generativelanguage.googleapis.com,api.openai.com"
    ).split(",")
    if h.strip()
}
MAX_MB = int(env("BT_PDF_MAX_MB", "100"))
PARALLEL_JOBS = int(env("BT_PDF_PARALLEL_JOBS", "1"))  # BabelDOC is CPU/RAM heavy: ~1 job per 2 cores / 4 GB
QUEUE_LIMIT = int(env("BT_PDF_QUEUE_LIMIT", "20"))
QPS = int(env("BT_PDF_QPS", "8"))  # parallel LLM requests per job
JOB_TIMEOUT = int(env("BT_PDF_JOB_TIMEOUT", "3600"))
JOB_TTL = int(env("BT_PDF_JOB_TTL", "3600"))
DATA_DIR = Path(env("BT_PDF_DATA_DIR", str(Path(tempfile.gettempdir()) / "bt-pdf-service")))
LANG_CODES = {"zh", "zh-TW", "en", "ja", "ko", "fr", "de", "es", "ru", "it", "pt", "auto"}


@dataclass
class Job:
    id: str
    dir: Path
    params: dict  # includes api_key: never logged, never written to disk
    dual: bool
    state: str = "queued"  # queued | running | done | error
    progress: float = 0.0
    stage: str = ""
    error: str = ""
    result: Path | None = None
    created: float = field(default_factory=time.time)
    finished: float | None = None
    task: asyncio.Task | None = None
    proc: asyncio.subprocess.Process | None = None


@asynccontextmanager
async def lifespan(_app):
    shutil.rmtree(DATA_DIR, ignore_errors=True)  # leftovers from a previous run hold user files
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cleaner = asyncio.get_running_loop().create_task(janitor())
    yield
    cleaner.cancel()
    for job in jobs.values():
        if job.proc and job.proc.returncode is None:
            job.proc.kill()


jobs: dict[str, "Job"] = {}
slots = asyncio.Semaphore(PARALLEL_JOBS)
app = FastAPI(title="BookTranslator PDF service", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=ORIGINS, allow_methods=["GET", "POST", "DELETE"], allow_headers=["*"])


def check_base_url(url: str) -> str:
    u = urlparse(url)
    if u.scheme != "https" or u.hostname not in LLM_HOSTS:
        raise HTTPException(400, f"不支持这个接口地址，允许：{', '.join(sorted(LLM_HOSTS))}")
    return url.rstrip("/")


def queue_position(job: Job) -> int:
    return sum(1 for j in jobs.values() if j.state == "queued" and j.created < job.created)


def drop(job: Job) -> None:
    jobs.pop(job.id, None)
    shutil.rmtree(job.dir, ignore_errors=True)


async def run_job(job: Job) -> None:
    try:
        await _run_job(job)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # never leave a job stuck in "running"
        if job.proc and job.proc.returncode is None:
            job.proc.kill()
        job.state, job.error, job.finished = "error", f"服务内部错误：{type(e).__name__}", time.time()
        print(f"job {job.id}: {e!r}", file=sys.stderr)


async def _run_job(job: Job) -> None:
    async with slots:
        if job.id not in jobs:
            return  # deleted while queued
        job.state = "running"
        job.proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "bt_pdf_service.worker",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            limit=1 << 20,
            stderr=asyncio.subprocess.DEVNULL if not os.environ.get("BT_PDF_DEBUG") else None,
            cwd=job.dir,
        )
        spec = {**job.params, "input": str(job.dir / "in.pdf"), "out_dir": str(job.dir / "out"), "work_dir": str(job.dir / "work"), "dual": job.dual, "qps": QPS}
        job.proc.stdin.write(json.dumps(spec).encode())
        job.proc.stdin.close()
        job.params.pop("api_key", None)  # the worker has it now
        try:
            async with asyncio.timeout(JOB_TIMEOUT):
                async for line in job.proc.stdout:
                    try:
                        ev = json.loads(line)
                    except ValueError:
                        continue  # stray output from a library
                    if ev["type"] == "progress":
                        job.progress, job.stage = ev["progress"], ev["stage"]
                    elif ev["type"] == "done":
                        job.result = Path(ev["file"])
                    elif ev["type"] == "error":
                        job.error = ev["error"]
                await job.proc.wait()
        except TimeoutError:
            job.proc.kill()
            job.error = f"超过 {JOB_TIMEOUT // 60} 分钟仍未完成，已停止"
        except asyncio.CancelledError:
            job.proc.kill()
            raise
        if job.result and job.result.exists():
            job.state, job.progress = "done", 100.0
        else:
            job.state = "error"
            job.error = job.error or f"翻译失败（退出码 {job.proc.returncode}）"
        job.finished = time.time()


async def janitor() -> None:
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for job in list(jobs.values()):
            if now - (job.finished or job.created) > JOB_TTL and job.state in ("done", "error"):
                drop(job)




@app.get("/")
def info() -> dict:
    return {
        "service": "BookTranslator PDF service",
        "engine": f"BabelDOC {version('babeldoc')}",
        "license": "AGPL-3.0",
        "source": SOURCE_URL,
        "max_mb": MAX_MB,
        "queue": sum(1 for j in jobs.values() if j.state in ("queued", "running")),
    }


@app.post("/jobs")
async def create_job(
    file: UploadFile,
    lang_out: str = Form(...),
    lang_in: str = Form("auto"),
    dual: bool = Form(False),
    base_url: str = Form(...),
    model: str = Form(...),
    api_key: str = Form(...),
) -> dict:
    if lang_out not in LANG_CODES or lang_in not in LANG_CODES or lang_out == "auto":
        raise HTTPException(400, "不支持的语言")
    base_url = check_base_url(base_url)
    if sum(1 for j in jobs.values() if j.state in ("queued", "running")) >= QUEUE_LIMIT:
        raise HTTPException(503, "服务器繁忙，请稍后再试")
    job_id = uuid.uuid4().hex
    job_dir = DATA_DIR / job_id
    job_dir.mkdir()
    size = 0
    with open(job_dir / "in.pdf", "wb") as f:
        while chunk := await file.read(1 << 20):
            size += len(chunk)
            if size > MAX_MB << 20:
                f.close()
                shutil.rmtree(job_dir, ignore_errors=True)
                raise HTTPException(413, f"文件超过 {MAX_MB} MB")
            f.write(chunk)
    with open(job_dir / "in.pdf", "rb") as f:
        if f.read(5) != b"%PDF-":
            shutil.rmtree(job_dir, ignore_errors=True)
            raise HTTPException(400, "不是 PDF 文件")
    job = Job(
        id=job_id,
        dir=job_dir,
        dual=dual,
        params={"lang_in": lang_in, "lang_out": lang_out, "base_url": base_url, "model": model, "api_key": api_key},
    )
    jobs[job_id] = job
    job.task = asyncio.get_running_loop().create_task(run_job(job))
    return {"id": job_id}


def get(job_id: str) -> Job:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "任务不存在或已过期")
    return job


@app.get("/jobs/{job_id}")
def status(job_id: str) -> dict:
    job = get(job_id)
    return {
        "state": job.state,
        "progress": job.progress,
        "stage": job.stage,
        "position": queue_position(job) if job.state == "queued" else 0,
        "error": job.error,
    }


@app.get("/jobs/{job_id}/result")
def result(job_id: str) -> FileResponse:
    job = get(job_id)
    if job.state != "done" or not job.result:
        raise HTTPException(409, "还没有完成")
    # keep it a little longer in case the download is retried, then the janitor removes it
    job.finished = min(job.finished or time.time(), time.time() - JOB_TTL + 600)
    return FileResponse(job.result, media_type="application/pdf", filename="translated.pdf")


@app.delete("/jobs/{job_id}")
async def delete(job_id: str) -> dict:
    job = get(job_id)
    if job.proc and job.proc.returncode is None:
        job.proc.kill()
    if job.task:
        job.task.cancel()
    drop(job)
    return {"ok": True}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=env("HOST", "127.0.0.1"), port=int(env("PORT", "8860")))


if __name__ == "__main__":
    main()
