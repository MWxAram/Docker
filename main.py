"""
Песочница для запуска чужого кода в Docker.
Запуск:  uvicorn main:app --port 8000
"""
import asyncio
import os
import shutil
import tempfile
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Optional

import docker
from docker.types import LogConfig
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------- настройки
RUN_MEMORY = "256m"          # лимит памяти для запуска пользовательского кода
COMPILE_MEMORY = "768m"      # компиляторам (javac, rustc, go) нужно больше
RUN_TIMEOUT = 10             # секунд на выполнение
COMPILE_TIMEOUT = 40         # секунд на компиляцию
MAX_PARALLEL = 4             # одновременных контейнеров
MAX_QUEUE = 100              # максимум задач в очереди
MAX_CODE_CHARS = 50_000      # максимальный размер кода
MAX_OUTPUT = 64 * 1024       # максимум байт stdout/stderr в ответе
MAX_JOBS_STORED = 1000       # сколько результатов хранить в памяти
WORK_ROOT = Path(os.getenv("SANDBOX_DIR", tempfile.gettempdir())) / "sandbox_jobs"

# ---------------------------------------------------------------- языки
# image   - образ Docker, file - имя файла с кодом,
# compile - команда компиляции (необязательно), run - команда запуска.
LANGUAGES = {
    "python": dict(image="python:3.12-slim", file="main.py",
                   run=["python", "/work/main.py"]),
    "javascript": dict(image="node:22-slim", file="main.js",
                       run=["node", "--max-old-space-size=1024", "/work/main.js"]),
    "typescript": dict(image="node:22-slim", file="main.ts",
                       run=["node", "--experimental-strip-types", "--max-old-space-size=1024",
                            "/work/main.ts"]),
    "java": dict(image="eclipse-temurin:21-jdk", file="Main.java",
                 compile=["javac", "-d", "/work", "/work/Main.java"],
                 run=["java", "-XX:-UsePerfData", "-Xmx1g", "-cp", "/work", "Main"]),
    "c": dict(image="gcc:14", file="main.c",
              compile=["gcc", "-O2", "-o", "/work/main", "/work/main.c", "-lm"],
              run=["/work/main"]),
    "cpp": dict(image="gcc:14", file="main.cpp",
                compile=["g++", "-O2", "-std=c++20", "-o", "/work/main", "/work/main.cpp"],
                run=["/work/main"]),
    "go": dict(image="golang:1.23", file="main.go",
               compile=["go", "build", "-o", "/work/main", "/work/main.go"],
               run=["/work/main"]),
    "rust": dict(image="rust:1-slim", file="main.rs",
                 compile=["rustc", "-O", "-o", "/work/main", "/work/main.rs"],
                 run=["/work/main"]),
    "ruby": dict(image="ruby:3.3-slim", file="main.rb", run=["ruby", "/work/main.rb"]),
    "php": dict(image="php:8.3-cli", file="main.php", run=["php", "-d", "memory_limit=-1", "/work/main.php"]),
    "bash": dict(image="bash:5", file="main.sh", run=["bash", "/work/main.sh"]),
}

app = FastAPI(title="Code Sandbox")
_client: Optional[docker.DockerClient] = None
_sem = asyncio.Semaphore(MAX_PARALLEL)
_jobs: "OrderedDict[str, dict]" = OrderedDict()


def client() -> docker.DockerClient:
    global _client
    if _client is None:
        _client = docker.from_env()
    return _client


# ---------------------------------------------------------------- Docker
def _exec(image, cmd, workdir, memory, timeout, pids, tmpfs):
    """Запускает один контейнер и возвращает результат. Контейнер всегда удаляется."""
    c = client().containers.create(
        image, cmd,
        working_dir="/work",
        volumes={workdir: {"bind": "/work", "mode": "rw"}},
        environment={"HOME": "/tmp", "GOCACHE": "/tmp/gocache", "GOPATH": "/tmp/go",
                     "CARGO_HOME": "/tmp/cargo"},
        mem_limit=memory,
        memswap_limit=memory,        # без swap, иначе лимит "не сработает"
        nano_cpus=1_000_000_000,     # 1 CPU
        pids_limit=pids,             # защита от fork-бомбы
        network_mode="none",         # без сети
        user="1000:1000",            # не root
        cap_drop=["ALL"],
        security_opt=["no-new-privileges"],
        read_only=True,
        tmpfs={"/tmp": tmpfs},
        log_config=LogConfig(type="json-file", config={"max-size": "1m", "max-file": "1"}),
    )
    started = time.monotonic()
    timed_out = False
    exit_code = None
    try:
        c.start()
        try:
            exit_code = c.wait(timeout=timeout)["StatusCode"]
        except Exception:            # таймаут ожидания
            timed_out = True
            try:
                c.kill()
            except Exception:
                pass
        elapsed = time.monotonic() - started
        c.reload()
        oom = bool(c.attrs["State"].get("OOMKilled"))
        out = c.logs(stdout=True, stderr=False)
        err = c.logs(stdout=False, stderr=True)
        return dict(
            exit_code=exit_code, timed_out=timed_out, oom=oom,
            stdout=out[:MAX_OUTPUT].decode(errors="replace"),
            stderr=err[:MAX_OUTPUT].decode(errors="replace"),
            truncated=len(out) > MAX_OUTPUT or len(err) > MAX_OUTPUT,
            time=round(elapsed, 3),
        )
    finally:
        try:
            c.remove(force=True)
        except Exception:
            pass


def _run_job(job: dict):
    lang = LANGUAGES[job["language"]]
    workdir = WORK_ROOT / job["id"]
    workdir.mkdir(parents=True, exist_ok=True)
    os.chmod(workdir, 0o777)  # контейнер работает под uid 1000
    try:
        (workdir / lang["file"]).write_text(job.pop("_code"), encoding="utf-8")
        os.chmod(workdir / lang["file"], 0o644)

        # 1) компиляция (отдельный контейнер с большим лимитом памяти)
        if "compile" in lang:
            r = _exec(lang["image"], lang["compile"], str(workdir),
                      COMPILE_MEMORY, COMPILE_TIMEOUT, pids=512, tmpfs="size=256m,exec")
            if r["timed_out"] or r["exit_code"] != 0:
                job.update(status="compilation_error", exit_code=r["exit_code"],
                           stdout=r["stdout"], stderr=r["stderr"] or "compilation timeout",
                           time=r["time"], truncated=r["truncated"])
                return

        # 2) запуск с лимитом 256 МБ
        r = _exec(lang["image"], lang["run"], str(workdir),
                  RUN_MEMORY, RUN_TIMEOUT, pids=128, tmpfs="size=16m")
        if r["timed_out"]:
            status = "timeout"
        elif r["oom"] or r["exit_code"] == 137:
            status = "memory_limit_exceeded"
        elif r["exit_code"] == 0:
            status = "ok"
        else:
            status = "runtime_error"
        job.update(status=status, exit_code=r["exit_code"], stdout=r["stdout"],
                   stderr=r["stderr"], time=r["time"], truncated=r["truncated"])
    except Exception as e:           # сбой самой песочницы, но не бэкенда
        job.update(status="internal_error", stderr=f"{type(e).__name__}: {e}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        job["state"] = "finished"


async def _process(job: dict):
    async with _sem:
        job["state"] = "running"
        await asyncio.to_thread(_run_job, job)


# ---------------------------------------------------------------- API
class RunRequest(BaseModel):
    language: str = Field(examples=["python"])
    code: str = Field(max_length=MAX_CODE_CHARS)


@app.get("/languages")
def languages():
    return sorted(LANGUAGES)


@app.get("/health")
def health():
    try:
        client().ping()
        return {"docker": "ok"}
    except Exception as e:
        raise HTTPException(503, f"docker unavailable: {e}")


@app.post("/run", status_code=202)
async def submit(req: RunRequest):
    if req.language not in LANGUAGES:
        raise HTTPException(400, f"unsupported language, choose from: {sorted(LANGUAGES)}")
    if not req.code.strip():
        raise HTTPException(400, "empty code")
    if sum(1 for j in _jobs.values() if j["state"] != "finished") >= MAX_QUEUE:
        raise HTTPException(429, "queue is full, try later")

    job = {"id": uuid.uuid4().hex, "state": "queued", "language": req.language,
           "status": None, "exit_code": None, "stdout": "", "stderr": "",
           "time": None, "truncated": False, "_code": req.code}
    _jobs[job["id"]] = job
    while len(_jobs) > MAX_JOBS_STORED:   # не копим историю бесконечно
        _jobs.popitem(last=False)
    asyncio.create_task(_process(job))
    return {"id": job["id"]}


@app.get("/run/{job_id}")
def result(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return {k: v for k, v in job.items() if not k.startswith("_")}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(Path(__file__).parent / "index.html")
