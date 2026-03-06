import argparse
import time
import threading
import os

import psutil


def monitor_cpu(proc: psutil.Process, samples: list[float], stop_flag: threading.Event, interval: float = 0.2):
    # Warm-up para que cpu_percent tenga base
    proc.cpu_percent(interval=None)
    while not stop_flag.is_set():
        samples.append(proc.cpu_percent(interval=interval))


def matmul_python(A, B):
    n = len(A)
    # C = A*B (triple loop clásico)
    C = [[0.0] * n for _ in range(n)]
    for i in range(n):
        Ai = A[i]
        Ci = C[i]
        for k in range(n):
            aik = Ai[k]
            Bk = B[k]
            for j in range(n):
                Ci[j] += aik * Bk[j]
    return C


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=500, help="Tamaño de la matriz NxN (subir/bajar según tu PC)")
    args = parser.parse_args()

    n = args.n
    # Matrices simples, determinísticas
    A = [[(i + j) % 10 for j in range(n)] for i in range(n)]
    B = [[(i * j) % 10 for j in range(n)] for i in range(n)]

    proc = psutil.Process(os.getpid())
    cpu_samples: list[float] = []
    stop = threading.Event()
    t = threading.Thread(target=monitor_cpu, args=(proc, cpu_samples, stop), daemon=True)
    t.start()

    t0 = time.perf_counter()
    _ = matmul_python(A, B)
    t1 = time.perf_counter()

    stop.set()
    t.join(timeout=1)

    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    peak_cpu = max(cpu_samples) if cpu_samples else 0.0

    print(f"[Python puro] N={n}")
    print(f"Tiempo (s): {t1 - t0:.3f}")
    print(f"CPU% promedio: {avg_cpu:.1f}")
    print(f"CPU% pico: {peak_cpu:.1f}")
    print("Nota: en muchas PCs esto queda cerca de ~100% (1 core).")


if __name__ == "__main__":
    main()