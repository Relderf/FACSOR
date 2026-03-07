import pandas as pd
import matplotlib.pyplot as plt

def cargar_csv(csv_path="server_log_v2.csv"):
    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()
    df["status_family"] = df["status_code"].astype(str).str[0] + "xx"
    return df

def mostrar_resumen(df):
    print("Filas:", len(df))
    print("Rango temporal:", df["timestamp"].min(), "->", df["timestamp"].max())
    print("Nodos:", sorted(df["server_node"].dropna().unique().tolist()))
    print("Tipos de evento:", sorted(df["event_type"].dropna().unique().tolist()))

def graficar_todo(df):
    tmp = df.copy()
    tmp["bucket_30m"] = tmp["timestamp"].dt.floor("30min")

    plt.figure(figsize=(11, 4))
    tmp.groupby("bucket_30m").size().plot()
    plt.title("Eventos por franja de 30 minutos")
    plt.xlabel("Franja horaria")
    plt.ylabel("Cantidad de eventos")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(11, 4))
    tmp.groupby("bucket_30m")["response_ms"].mean().plot()
    plt.title("Tiempo de respuesta promedio por franja")
    plt.xlabel("Franja horaria")
    plt.ylabel("response_ms")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(6, 6))
    tmp["event_type"].value_counts().plot(kind="pie", autopct="%1.0f%%")
    plt.title("Distribución de tipos de evento")
    plt.ylabel("")
    plt.tight_layout()
    plt.show()

    status_by_bucket = tmp.groupby(["bucket_30m", "status_family"]).size().unstack(fill_value=0)
    plt.figure(figsize=(11, 4))
    status_by_bucket.plot(kind="bar", stacked=True, ax=plt.gca(), width=0.9)
    plt.title("Códigos de estado por franja")
    plt.xlabel("Franja horaria")
    plt.ylabel("Cantidad")
    plt.xticks([])
    plt.tight_layout()
    plt.show()

def ejecutar_analisis(csv_path="server_log_v2.csv"):
    df = cargar_csv(csv_path)
    mostrar_resumen(df)
    graficar_todo(df)
    return df
