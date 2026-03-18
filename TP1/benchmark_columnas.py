import csv
import time
import numpy as np
import pandas as pd
import polars as pl
from numba import njit

# Cantidad de veces que se repite cada método
ITERACIONES = 1000
CSV_FILE = "poblacion_ciudades.csv"


def cargar_datos(csv_file):
    ciudades = []
    poblaciones = []

    with open(csv_file, newline="", encoding="utf-8") as archivo:
        lector = csv.DictReader(archivo)
        for fila in lector:
            ciudades.append(fila["ciudad"])
            poblaciones.append(int(fila["poblacion"]))
    return ciudades, poblaciones


def python_puro(ciudades, poblaciones):
    resultado = []
    for i in range(len(poblaciones)):
        resultado.append(poblaciones[i] * 2)
    return resultado


def numpy_version(poblaciones):
    arr = np.array(poblaciones)
    return arr * 2


def pandas_version(csv_file):
    df = pd.read_csv(csv_file)
    df["poblacion_x2"] = df["poblacion"] * 2
    return df


def polars_version(csv_file):
    df = pl.read_csv(csv_file)
    df = df.with_columns((pl.col("poblacion") * 2).alias("poblacion_x2"))
    return df


@njit
def numba_kernel(arr):
    out = np.empty_like(arr)
    for i in range(len(arr)):
        out[i] = arr[i] * 2
    return out


def numba_version(poblaciones):
    arr = np.array(poblaciones)
    return numba_kernel(arr)


def medir(nombre, funcion):
    inicio = time.perf_counter()
    for _ in range(ITERACIONES):
        resultado = funcion()
    fin = time.perf_counter()
    tiempo = (fin - inicio) * 1000
    print(f"{nombre}: {tiempo:.3f} ms (total en {ITERACIONES} iteraciones)")
    return resultado


def main():

    ciudades, poblaciones = cargar_datos(CSV_FILE)

    print("Benchmark: duplicar columna poblacion")
    print(f"Filas: {len(poblaciones)}")
    print(f"Iteraciones: {ITERACIONES}")
    print()

    # warmup numba
    numba_kernel(np.array(poblaciones))

    r1 = medir("Python puro", lambda: python_puro(ciudades, poblaciones))
    r2 = medir("NumPy", lambda: numpy_version(poblaciones))
    r3 = medir("Pandas", lambda: pandas_version(CSV_FILE))
    r4 = medir("Polars", lambda: polars_version(CSV_FILE))
    r5 = medir("Numba", lambda: numba_version(poblaciones))


if __name__ == "__main__":
    main()