# Visualización de datos en tiempo real para datasets

Aplicación Flask para comparar rendimiento entre modos de ejecución y mostrar cada gráfica en tiempo real:

- Ejecución secuencial (single thread)
- Ejecución paralela con hilos (multi thread)
- Ejecución paralela con procesos (multi core)

## Requisitos

- Python 3.10+

## Instalacion

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecucion

```bash
python app.py
```

Abrir en el navegador: `http://127.0.0.1:5000`

## Métricas calculadas

- Tiempo de ejecucion
- Speedup
- Uso de CPU estimado
- Eficiencia paralela

## Visualización

La interfaz usa Server-Sent Events para ir enviando los resultados de cada modo a medida que se calculan y actualizar las gráficas sin recargar la página.

## Dataset

Se incluye un CSV pequeno de ejemplo en `data/iris_small.csv`.
