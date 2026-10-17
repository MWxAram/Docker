"""
Песочница для запуска чужого кода в Docker.
Запуск:  uvicorn main:app --port 8000
"""
import asyncio
import base64
import io
import os
import shutil
import stat
import tempfile
import time
import uuid
import zipfile
from collections import OrderedDict
from pathlib import Path, PurePosixPath
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
MAX_UPLOAD_BYTES = 50 * 2**20    # всего загружаемых файлов за один запуск
MAX_EXTRACTED = 150 * 2**20      # лимит после распаковки ZIP (больше - отмена)
MAX_FILES = 2000                 # максимум файлов в одном запуске
MAX_STAGED_BYTES = 1024 * 2**20  # сколько файлов одновременно ждёт в очереди на диске
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
                 compile=["sh", "-c", "javac -d /work $(find /work -name '*.java')"],
                 run=["java", "-XX:-UsePerfData", "-Xmx1g", "-cp", "/work", "Main"]),
    "c": dict(image="gcc:14", file="main.c",
              compile=["sh", "-c", "gcc -O2 -o /work/main /work/*.c -lm"],
              run=["/work/main"]),
    "cpp": dict(image="gcc:14", file="main.cpp",
                compile=["sh", "-c", "g++ -O2 -std=c++20 -o /work/main /work/*.cpp"],
                run=["/work/main"]),
    "go": dict(image="golang:1.23", file="main.go",
               compile=["sh", "-c", "go build -o /work/main /work/*.go"],
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
def _ensure_image(image):
    """Если образа нет локально, скачиваем его (при первом запуске языка)."""
    try:
        client().images.get(image)
    except docker.errors.ImageNotFound:
        client().images.pull(image)


def _exec(image, cmd, workdir, memory, timeout, pids, tmpfs, mode="rw"):
    """Запускает один контейнер и возвращает результат. Контейнер всегда удаляется."""
    _ensure_image(image)
    c = client().containers.create(
        image, cmd,
        working_dir="/work",
        volumes={workdir: {"bind": "/work", "mode": mode}},
        environment={"HOME": "/tmp", "GOCACHE": "/tmp/gocache", "GOPATH": "/tmp/go",
                     "CARGO_HOME": "/tmp/cargo", "PYTHONDONTWRITEBYTECODE": "1"},
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
    workdir = Path(job["_dir"])
    try:
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
                  RUN_MEMORY, RUN_TIMEOUT, pids=128, tmpfs="size=16m", mode="ro")
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
class UploadError(Exception):
    """Ошибка в загруженных файлах: возвращается пользователю как 400."""


class _Budget:
    def __init__(self):
        self.used = 0
        self.files = 0

    def add(self, n):
        self.used += n
        if self.used > MAX_EXTRACTED:
            raise UploadError(
                f"Файлы занимают больше {MAX_EXTRACTED // 2**20} МБ после распаковки: загрузка отменена")


def _safe_path(root: Path, name: str) -> Optional[Path]:
    """Путь внутри root или None (защита от ../ и абсолютных путей)."""
    name = name.replace("\\", "/").lstrip("/")
    p = PurePosixPath(name)
    if not p.parts or ".." in p.parts or "\x00" in name:
        return None
    target = (root / p).resolve()
    return target if target.is_relative_to(root.resolve()) else None


def _unzip(raw: bytes, root: Path, budget: _Budget):
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise UploadError("Файл .zip повреждён или не является архивом")
    with zf:
        infos = [i for i in zf.infolist() if not i.is_dir()
                 and not i.filename.startswith("__MACOSX/") and not i.filename.endswith(".DS_Store")]
        if budget.files + len(infos) > MAX_FILES:
            raise UploadError(f"Слишком много файлов (максимум {MAX_FILES})")
        # быстрая отмена по заявленным размерам ...
        if budget.used + sum(i.file_size for i in infos) > MAX_EXTRACTED:
            raise UploadError(
                f"Архив распаковывается больше чем в {MAX_EXTRACTED // 2**20} МБ: распаковка отменена")
        for info in infos:
            if info.flag_bits & 0x1:
                raise UploadError("Архив защищён паролем")
            if stat.S_ISLNK(info.external_attr >> 16):
                continue                      # симлинки не распаковываем
            target = _safe_path(root, info.filename)
            if target is None:
                raise UploadError(f"Недопустимый путь в архиве: {info.filename}")
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(target, "wb") as dst:
                    # ... и настоящий подсчёт байт: заголовок архива может врать
                    while chunk := src.read(1 << 16):
                        budget.add(len(chunk))
                        dst.write(chunk)
            except (OSError, zipfile.BadZipFile, RuntimeError, NotImplementedError):
                raise UploadError(f"Не удалось распаковать {info.filename}")
            budget.files += 1


def _stage(job_id: str, req: "RunRequest"):
    """Раскладывает загруженные файлы и код по рабочей папке. Любая ошибка = папка удаляется."""
    root = WORK_ROOT / job_id
    root.mkdir(parents=True)
    budget = _Budget()
    try:
        if sum(len(f.data) * 3 // 4 for f in req.files) > MAX_UPLOAD_BYTES:
            raise UploadError(f"Загруженные файлы больше {MAX_UPLOAD_BYTES // 2**20} МБ")
        for f in req.files:
            try:
                raw = base64.b64decode(f.data, validate=True)
            except ValueError:
                raise UploadError(f"Файл {f.name} передан некорректно")
            if f.name.lower().endswith(".zip"):
                _unzip(raw, root, budget)
            else:
                target = _safe_path(root, f.name)
                if target is None:
                    raise UploadError(f"Недопустимое имя файла: {f.name}")
                budget.add(len(raw))
                budget.files += 1
                if budget.files > MAX_FILES:
                    raise UploadError(f"Слишком много файлов (максимум {MAX_FILES})")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
        # код из редактора записывается последним и имеет приоритет над одноимённым файлом
        (root / LANGUAGES[req.language]["file"]).write_text(req.code, encoding="utf-8")
        for dirpath, _dirs, names in os.walk(root):   # контейнер работает под uid 1000
            os.chmod(dirpath, 0o777)
            for n in names:
                os.chmod(os.path.join(dirpath, n), 0o644)
        return root, budget.used
    except BaseException:
        shutil.rmtree(root, ignore_errors=True)
        raise


class FileIn(BaseModel):
    name: str = Field(max_length=255)
    data: str                       # содержимое в base64


class RunRequest(BaseModel):
    language: str = Field(examples=["python"])
    code: str = Field(max_length=MAX_CODE_CHARS)
    files: list[FileIn] = Field(default_factory=list, max_length=MAX_FILES)


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
    unfinished = [j for j in _jobs.values() if j["state"] != "finished"]
    if len(unfinished) >= MAX_QUEUE or sum(j["_bytes"] for j in unfinished) > MAX_STAGED_BYTES:
        raise HTTPException(429, "queue is full, try later")

    job_id = uuid.uuid4().hex
    try:
        workdir, size = await asyncio.to_thread(_stage, job_id, req)
    except UploadError as e:
        raise HTTPException(400, str(e))
    except OSError:
        raise HTTPException(400, "Не удалось сохранить файлы")

    job = {"id": job_id, "state": "queued", "language": req.language,
           "status": None, "exit_code": None, "stdout": "", "stderr": "",
           "time": None, "truncated": False, "_dir": str(workdir), "_bytes": size}
    _jobs[job_id] = job
    while len(_jobs) > MAX_JOBS_STORED:   # не копим историю бесконечно
        _jobs.popitem(last=False)
    asyncio.create_task(_process(job))
    return {"id": job_id}


@app.get("/run/{job_id}")
def result(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return {k: v for k, v in job.items() if not k.startswith("_")}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(Path(__file__).parent / "index.html")