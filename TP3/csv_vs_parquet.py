import time
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent

CSV_PATH = BASE_DIR / "ventas_logistica.csv"
PARQUET_PATH = BASE_DIR / "ventas_logistica.parquet"

CSV_OUT = BASE_DIR / "ventas_logistica_modificado.csv"
PARQUET_OUT = BASE_DIR / "ventas_logistica_modificado.parquet"


def transformar_datos(df: pd.DataFrame) -> pd.DataFrame:
    df_mod = df.copy()
    df_mod["importe_con_recargo"] = (df_mod["importe"] * 1.07).round(2)
    df_mod["producto_normalizado"] = df_mod["producto"].astype(str).str.upper()
    return df_mod


def ejecutar_benchmark(nombre: str, archivo_entrada: Path, archivo_salida: Path, lector, escritor) -> None:
    print(nombre)

    t0 = time.perf_counter()
    df = lector(archivo_entrada)
    t1 = time.perf_counter()

    df_mod = transformar_datos(df)

    t2 = time.perf_counter()
    escritor(df_mod, archivo_salida)
    t3 = time.perf_counter()

    print(f"    Lectura:            {t1 - t0:.6f} s")
    print(f"    Escritura:          {t3 - t2:.6f} s")
    print()


def leer_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def escribir_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def leer_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def escribir_parquet(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, index=False)


if __name__ == "__main__":
    ejecutar_benchmark("CSV", CSV_PATH, CSV_OUT, leer_csv, escribir_csv)
    ejecutar_benchmark("Parquet", PARQUET_PATH, PARQUET_OUT, leer_parquet, escribir_parquet)
