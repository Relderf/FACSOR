import gzip
import shutil
import os
import time

N_REPETICIONES = 5

archivo_original = "api_requests.csv.gz"
archivo_descomprimido = "api_requests_descomprimido.csv"
archivo_recomprimido = "api_requests_recomprimido.csv.gz"

# DESCOMPRESIÓN
tiempo_total_descomp = 0
for _ in range(N_REPETICIONES):
    inicio_descomp = time.time()

    with gzip.open(archivo_original, "rb") as f_in:
        with open(archivo_descomprimido, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    fin_descomp = time.time()
    tiempo_total_descomp += fin_descomp - inicio_descomp

# RECOMPRESIÓN
tiempo_total_recomp = 0
for _ in range(N_REPETICIONES):
    inicio_recomp = time.time()

    with open(archivo_descomprimido, "rb") as f_in:
        with gzip.open(archivo_recomprimido, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    fin_recomp = time.time()
    tiempo_total_recomp += fin_recomp - inicio_recomp

# TAMAÑOS
tam_original = os.path.getsize(archivo_original)
tam_descomprimido = os.path.getsize(archivo_descomprimido)
tam_recomprimido = os.path.getsize(archivo_recomprimido)

print("Tiempo descompresión:", round(fin_descomp - inicio_descomp, 3), "seg")
print("Tiempo recompresión:", round(fin_recomp - inicio_recomp, 3), "seg")
print("Repeticiones por etapa:", N_REPETICIONES)
print("-" * 30)
print("Tiempo total descompresión:", round(tiempo_total_descomp, 3), "seg")
print("Tiempo promedio descompresión:", round(tiempo_total_descomp / N_REPETICIONES, 3), "seg")
print("Tiempo total recompresión:", round(tiempo_total_recomp, 3), "seg")
print("Tiempo promedio recompresión:", round(tiempo_total_recomp / N_REPETICIONES, 3), "seg")
print("-" * 30)
print("Tamaño original:", round(tam_original / (1024*1024), 2), "MB")
print("Tamaño descomprimido:", round(tam_descomprimido / (1024*1024), 2), "MB")
print("Tamaño recomprimido:", round(tam_recomprimido / (1024*1024), 2), "MB")