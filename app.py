import csv
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, Generator, List

import psutil
from flask import Flask, Response, render_template, request, stream_with_context

app = Flask(__name__)


@app.route("/favicon.ico")
def favicon() -> Response:
    return Response(status=204)


@app.route("/benchmark-stream")
def benchmark_stream() -> Response:
    default_dataset = os.path.join("data", "iris_small.csv")
    dataset_path = request.args.get("dataset_path", default_dataset).strip()
    workers = int(request.args.get("workers", "2"))
    intensity = int(request.args.get("intensity", "400"))
    repeats = int(request.args.get("repeats", "2"))

    @stream_with_context
    def generate() -> Generator[str, None, None]:
        try:
            yield _sse("status", {"message": "Iniciando visualizacion de datos en tiempo real..."})
            yield from stream_benchmark(dataset_path, workers, intensity, repeats)
        except Exception as exc:
            yield _sse("stream-error", {"message": f"Error al ejecutar benchmark: {exc}"})

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@dataclass
class BenchmarkResult:
    mode: str
    workers: int
    time_seconds: float
    cpu_percent: float
    checksum: float


def load_numeric_rows(csv_path: str) -> List[List[float]]:
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    if not rows:
        raise ValueError("El CSV esta vacio.")

    numeric_columns: List[str] = []
    first_row = rows[0]
    for key, value in first_row.items():
        try:
            float(value)
            numeric_columns.append(key)
        except (TypeError, ValueError):
            continue

    if not numeric_columns:
        raise ValueError("El CSV no contiene columnas numericas.")

    numeric_rows: List[List[float]] = []
    for row in rows:
        numeric_rows.append([float(row[column]) for column in numeric_columns])
    return numeric_rows


def split_chunks(rows: List[List[float]], workers: int) -> List[List[List[float]]]:
    if workers <= 1 or len(rows) < workers:
        return [rows]

    chunk_size = max(1, len(rows) // workers)
    chunks: List[List[List[float]]] = []
    start = 0
    while start < len(rows):
        end = min(len(rows), start + chunk_size)
        chunks.append(rows[start:end])
        start = end
    return chunks


def heavy_compute_chunk(chunk: List[List[float]], intensity: int) -> float:
    total = 0.0
    for row in chunk:
        for value in row:
            local = abs(value) + 1.0
            # Trabajo CPU-bound artificial para observar diferencias entre modos.
            for _ in range(intensity):
                local = math.sqrt(local * 1.00001) + math.log1p(local)
            total += local
    return total


def heavy_compute_chunk_with_cpu(args: tuple[List[List[float]], int]) -> tuple[float, float]:
    chunk, intensity = args
    cpu_start = time.process_time()
    checksum = heavy_compute_chunk(chunk, intensity)
    cpu_time = time.process_time() - cpu_start
    return checksum, cpu_time


def run_once(mode: str, rows: List[List[float]], workers: int, intensity: int) -> BenchmarkResult:
    chunks = split_chunks(rows, workers)
    process = psutil.Process()
    cpu_start = process.cpu_times()
    t0 = time.perf_counter()
    worker_cpu_time = 0.0

    if mode == "sequential":
        checksum = heavy_compute_chunk(rows, intensity)
    elif mode == "thread":
        with ThreadPoolExecutor(max_workers=workers) as executor:
            checksum = sum(executor.map(heavy_compute_chunk, chunks, [intensity] * len(chunks)))
    elif mode == "process":
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(
                executor.map(
                    heavy_compute_chunk_with_cpu,
                    [(chunk, intensity) for chunk in chunks],
                )
            )
        checksum = sum(result[0] for result in results)
        worker_cpu_time = sum(result[1] for result in results)
    else:
        raise ValueError(f"Modo no soportado: {mode}")

    elapsed = time.perf_counter() - t0
    if mode == "process":
        cpu_time_delta = worker_cpu_time
    else:
        cpu_end = process.cpu_times()
        cpu_time_delta = (cpu_end.user + cpu_end.system) - (cpu_start.user + cpu_start.system)
    cpu_percent = (cpu_time_delta / elapsed) * 100 if elapsed > 0 else 0.0

    return BenchmarkResult(
        mode=mode,
        workers=workers,
        time_seconds=elapsed,
        cpu_percent=cpu_percent,
        checksum=checksum,
    )


def benchmark(mode: str, rows: List[List[float]], workers: int, intensity: int, repeats: int) -> BenchmarkResult:
    samples = [run_once(mode, rows, workers, intensity) for _ in range(repeats)]
    avg_time = sum(s.time_seconds for s in samples) / len(samples)
    avg_cpu = sum(s.cpu_percent for s in samples) / len(samples)
    return BenchmarkResult(mode, workers, avg_time, avg_cpu, samples[-1].checksum)


def to_metrics(sequential: BenchmarkResult, other: BenchmarkResult) -> Dict[str, float]:
    speedup = sequential.time_seconds / other.time_seconds if other.time_seconds > 0 else 0.0
    efficiency = speedup / other.workers if other.workers > 0 else 0.0
    return {
        "speedup": speedup,
        "efficiency": efficiency,
    }


def build_results(
    sequential: BenchmarkResult,
    threaded: BenchmarkResult,
    multicore: BenchmarkResult,
    workers: int,
) -> Dict[str, Dict[str, float]]:
    thread_metrics = to_metrics(sequential, threaded)
    process_metrics = to_metrics(sequential, multicore)

    return {
        "sequential": {
            "label": "Single Thread",
            "workers": 1,
            "time_seconds": sequential.time_seconds,
            "cpu_percent": sequential.cpu_percent,
            "speedup": 1.0,
            "efficiency": 1.0,
        },
        "thread": {
            "label": "Multi Thread",
            "workers": workers,
            "time_seconds": threaded.time_seconds,
            "cpu_percent": threaded.cpu_percent,
            "speedup": thread_metrics["speedup"],
            "efficiency": thread_metrics["efficiency"],
        },
        "process": {
            "label": "Multi Core",
            "workers": workers,
            "time_seconds": multicore.time_seconds,
            "cpu_percent": multicore.cpu_percent,
            "speedup": process_metrics["speedup"],
            "efficiency": process_metrics["efficiency"],
        },
    }


def _sse(event_name: str, payload: Dict[str, object]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=True)}\n\n"


def stream_benchmark(dataset_path: str, workers: int, intensity: int, repeats: int) -> Generator[str, None, None]:
    rows = load_numeric_rows(dataset_path)

    sequential = benchmark("sequential", rows, 1, intensity, repeats)
    yield _sse(
        "result",
        {
            "mode": sequential.mode,
            "label": "Single Thread",
            "workers": sequential.workers,
            "time_seconds": sequential.time_seconds,
            "cpu_percent": sequential.cpu_percent,
            "checksum": sequential.checksum,
            "speedup": 1.0,
            "efficiency": 1.0,
        },
    )

    threaded = benchmark("thread", rows, workers, intensity, repeats)
    thread_metrics = to_metrics(sequential, threaded)
    yield _sse(
        "result",
        {
            "mode": threaded.mode,
            "label": "Multi Thread",
            "workers": threaded.workers,
            "time_seconds": threaded.time_seconds,
            "cpu_percent": threaded.cpu_percent,
            "checksum": threaded.checksum,
            "speedup": thread_metrics["speedup"],
            "efficiency": thread_metrics["efficiency"],
        },
    )

    multicore = benchmark("process", rows, workers, intensity, repeats)
    process_metrics = to_metrics(sequential, multicore)
    yield _sse(
        "result",
        {
            "mode": multicore.mode,
            "label": "Multi Core",
            "workers": multicore.workers,
            "time_seconds": multicore.time_seconds,
            "cpu_percent": multicore.cpu_percent,
            "checksum": multicore.checksum,
            "speedup": process_metrics["speedup"],
            "efficiency": process_metrics["efficiency"],
        },
    )

    yield _sse("done", build_results(sequential, threaded, multicore, workers))


@app.route("/", methods=["GET", "POST"])
def index():
    default_dataset = os.path.join("data", "iris_small.csv")
    context = {
        "dataset_path": default_dataset,
        "workers": max(2, os.cpu_count() // 2 if os.cpu_count() else 2),
        "intensity": 400,
        "repeats": 2,
        "error": None,
        "results": None,
    }

    if request.method == "POST":
        dataset_path = request.form.get("dataset_path", default_dataset).strip()
        workers = int(request.form.get("workers", "2"))
        intensity = int(request.form.get("intensity", "400"))
        repeats = int(request.form.get("repeats", "2"))

        context.update(
            {
                "dataset_path": dataset_path,
                "workers": workers,
                "intensity": intensity,
                "repeats": repeats,
            }
        )

        try:
            rows = load_numeric_rows(dataset_path)
            sequential = benchmark("sequential", rows, 1, intensity, repeats)
            threaded = benchmark("thread", rows, workers, intensity, repeats)
            multicore = benchmark("process", rows, workers, intensity, repeats)
            context["results"] = build_results(sequential, threaded, multicore, workers)

        except Exception as exc:
            context["error"] = f"Error al ejecutar benchmark: {exc}"

    return render_template("index.html", **context)


if __name__ == "__main__":
    app.run(debug=False, use_reloader=False)
