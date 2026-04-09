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


GLOSSARY_TERMS = {
    "A": [
        {
            "term": "Analizador",
            "definition": "Sistema que ejecuta pruebas y compara el comportamiento de distintos modos de procesamiento.",
        }
    ],
    "B": [
        {
            "term": "Benchmark",
            "definition": "Prueba controlada para medir tiempos, uso de CPU y eficiencia entre modos de ejecucion.",
        },
        {
            "term": "Bloque de Datos",
            "definition": "Subconjunto del dataset asignado a un worker para procesamiento paralelo.",
        },
    ],
    "C": [
        {
            "term": "CPU",
            "definition": "Unidad de procesamiento que ejecuta instrucciones y reparte trabajo entre hilos o nucleos.",
        },
        {
            "term": "Checksum",
            "definition": "Valor numerico de control que valida que los modos calculan resultados consistentes.",
        },
        {
            "term": "Core",
            "definition": "Nucleo fisico o logico de la CPU capaz de procesar tareas en paralelo.",
        },
    ],
    "D": [
        {
            "term": "Dataset",
            "definition": "Conjunto de datos de entrada usado por el simulador para calcular metricas y comparar modos.",
        },
        {
            "term": "Data Particle",
            "definition": "Elemento visual animado que representa una unidad de trabajo moviendose por el simulador.",
        },
    ],
    "E": [
        {
            "term": "Escalabilidad",
            "definition": "Capacidad de mejorar rendimiento cuando se incrementan workers, hilos o procesos.",
        },
        {
            "term": "Eficiencia",
            "definition": "Relacion entre speedup y cantidad de workers. Indica que tan bien se aprovecha el paralelismo.",
        },
        {
            "term": "Event Stream",
            "definition": "Canal de eventos en tiempo real (SSE) usado para actualizar la interfaz durante la ejecucion.",
        },
    ],
    "G": [
        {
            "term": "GIL",
            "definition": "Global Interpreter Lock de Python que limita ejecucion simultanea de bytecode en hilos CPU-bound.",
        }
    ],
    "H": [
        {
            "term": "Hilo",
            "definition": "Unidad de ejecucion liviana dentro de un proceso que comparte memoria con otros hilos.",
        },
        {
            "term": "Hardware Simulation",
            "definition": "Representacion visual del reparto de trabajo entre cores durante el benchmark.",
        },
    ],
    "I": [
        {
            "term": "Intensidad",
            "definition": "Nivel de carga computacional aplicado a cada dato para hacer visibles diferencias de rendimiento.",
        }
    ],
    "L": [
        {
            "term": "Latencia",
            "definition": "Tiempo de respuesta observado para completar una tarea o etapa del procesamiento.",
        },
        {
            "term": "Linea Base",
            "definition": "Referencia inicial, normalmente el modo secuencial, para calcular speedup y eficiencia.",
        },
    ],
    "M": [
        {
            "term": "Multi Core",
            "definition": "Ejecucion en multiples procesos para distribuir carga entre nucleos y evitar limites del GIL.",
        },
        {
            "term": "Multi Thread",
            "definition": "Ejecucion en multiples hilos dentro del mismo proceso. Comparte memoria y reduce sobrecarga de comunicacion.",
        },
        {
            "term": "Metrica",
            "definition": "Valor cuantitativo que describe rendimiento, por ejemplo tiempo, CPU, speedup o eficiencia.",
        },
    ],
    "P": [
        {
            "term": "Paralelismo",
            "definition": "Ejecucion concurrente de tareas para reducir tiempo total de procesamiento.",
        },
        {
            "term": "Proceso",
            "definition": "Instancia independiente del programa con memoria separada y planificacion propia del sistema operativo.",
        },
    ],
    "R": [
        {
            "term": "Rendimiento",
            "definition": "Capacidad de completar trabajo en menor tiempo y con uso eficiente de recursos de hardware.",
        },
        {
            "term": "Repeticiones",
            "definition": "Cantidad de ejecuciones por modo para calcular promedios y reducir ruido de medicion.",
        },
    ],
    "S": [
        {
            "term": "Simulador",
            "definition": "Interfaz que representa visualmente como se distribuye la carga entre hilos y procesos en tiempo real.",
        },
        {
            "term": "Single Thread",
            "definition": "Modo secuencial que ejecuta todo en un solo hilo y sirve como linea base de comparacion.",
        },
        {
            "term": "Speedup",
            "definition": "Cuantas veces es mas rapido un modo paralelo frente al modo secuencial.",
        },
        {
            "term": "SSE",
            "definition": "Server-Sent Events, mecanismo para enviar resultados del backend al navegador en tiempo real.",
        },
    ],
    "T": [
        {
            "term": "Throughput",
            "definition": "Cantidad de trabajo procesado por unidad de tiempo en una configuracion dada.",
        },
        {
            "term": "Tiempo de Ejecucion",
            "definition": "Duracion total de una prueba desde su inicio hasta la entrega de resultados.",
        },
    ],
    "W": [
        {
            "term": "Worker",
            "definition": "Unidad de trabajo (hilo o proceso) dedicada a procesar una porcion del dataset.",
        }
    ],
}


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


@app.route("/glosario")
def glossary():
    letters = list(GLOSSARY_TERMS.keys())
    total_terms = sum(len(terms) for terms in GLOSSARY_TERMS.values())
    return render_template(
        "glossary.html",
        glossary_terms=GLOSSARY_TERMS,
        glossary_letters=letters,
        glossary_total=total_terms,
    )


if __name__ == "__main__":
    app.run(debug=False, use_reloader=False)
