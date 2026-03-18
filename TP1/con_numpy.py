import argparse
import time
import threading
import os

import numpy as np
import psutil


def monitor_cpu(proc: psutil.Process, samples: list[float], stop_flag: threading.Event, interval: float = 0.2):
    proc.cpu_percent(interval=None)
    while not stop_flag.is_set():
        samples.append(proc.cpu_percent(interval=interval))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=500, help="Tamaño de la matriz NxN (subir/bajar según tu PC)")
    args = parser.parse_args()

    n = args.n
    # Matrices simples
    A = np.fromfunction(lambda i, j: (i + j) % 10, (n, n), dtype=float)
    B = np.fromfunction(lambda i, j: (i * j) % 10, (n, n), dtype=float)

    proc = psutil.Process(os.getpid())
    cpu_samples: list[float] = []
    stop = threading.Event()
    t = threading.Thread(target=monitor_cpu, args=(proc, cpu_samples, stop), daemon=True)
    t.start()

    t0 = time.perf_counter()
    _ = A @ B  # BLAS / vectorización interna
    t1 = time.perf_counter()

    stop.set()
    t.join(timeout=1)

    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    peak_cpu = max(cpu_samples) if cpu_samples else 0.0

    print(f"[NumPy] N={n}")
    print(f"Tiempo (s): {t1 - t0:.3f}")
    print(f"CPU% promedio: {avg_cpu:.1f}")
    print(f"CPU% pico: {peak_cpu:.1f}")
    print("Nota: puede dar >100% si NumPy usa múltiples núcleos (depende del BLAS instalado).")


if __name__ == "__main__":
    main()