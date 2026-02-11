import pandas as pd
import requests
import zipfile
import io
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================================================
# CONFIG
# =========================================================
GTFS_SOURCES = [
    {"nom": "SNCF France", "url": "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"},
]

OUTPUT_FILE = "data/trains_europe_summary.csv"

# Charge un .env si tu en crées un (optionnel)
load_dotenv()

# Mets ton mot de passe postgres ici si tu veux (ou via .env)
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/obrail")

# =========================================================
# UTILS
# =========================================================
def parse_hhmmss_to_seconds(t: str):
    """Convertit HH:MM:SS en secondes, supporte HH>24 (GTFS)."""
    try:
        h, m, s = t.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except Exception:
        return None

def compute_is_night(dep_time: str, arr_time: str) -> bool:
    """Règle simple: nuit si départ >= 20:00 ou arrivée <= 06:00."""
    dep_s = parse_hhmmss_to_seconds(dep_time)
    arr_s = parse_hhmmss_to_seconds(arr_time)
    if dep_s is None or arr_s is None:
        return False
    return (dep_s >= 20 * 3600) or (arr_s <= 6 * 3600)

# =========================================================
# ETL RUN LOG
# =========================================================
def start_etl_run(engine, source_name: str, source_url: str) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            text("""INSERT INTO obrail.etl_run(source_name, source_url)
                    VALUES (:n, :u) RETURNING run_id"""),
            {"n": source_name, "u": source_url}
        )
        return res.scalar()

def finish_etl_run(engine, run_id: int, status: str, rows_extracted: int, rows_loaded: int, error_message: str | None = None):
    with engine.begin() as conn:
        conn.execute(
            text("""UPDATE obrail.etl_run
                    SET finished_at = now(),
                        status = :st,
                        rows_extracted = :re,
                        rows_loaded = :rl,
                        error_message = :err
                    WHERE run_id = :id"""),
            {"st": status, "re": rows_extracted, "rl": rows_loaded, "err": error_message, "id": run_id}
        )

# =========================================================
# 1) EXTRACT
# =========================================================
def download_gtfs(url, nom_source):
    print(f"📥 Téléchargement GTFS depuis {nom_source} : {url}")
    if not url.startswith("http"):
        print(f"   ⚠️ Pas un lien téléchargeable : {url}")
        return None
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        print(f"   ✅ Téléchargement réussi pour {nom_source}")
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        print(f"   ❌ ERREUR HTTP pour {nom_source} : {e}")
        return None

# =========================================================
# 2) TRANSFORM
# =========================================================
def transform_gtfs_to_summary(gtfs_bytes, nom_source):
    if not gtfs_bytes:
        return pd.DataFrame()

    print(f"🧹 Transformation GTFS {nom_source} (summary départ/arrivée)...")

    try:
        with zipfile.ZipFile(gtfs_bytes) as z:
            trips = pd.read_csv(z.open("trips.txt"))
            stop_times = pd.read_csv(z.open("stop_times.txt"))
            stops = pd.read_csv(z.open("stops.txt"))
    except KeyError as e:
        print(f"   ⚠️ Fichier manquant dans {nom_source} : {e}")
        return pd.DataFrame()

    # Jointure pour avoir stop_name
    df = stop_times.merge(trips[["trip_id"]], on="trip_id", how="inner")
    df = df.merge(stops[["stop_id", "stop_name"]], on="stop_id", how="left")

    df = df[["trip_id", "stop_sequence", "stop_name", "arrival_time", "departure_time"]].copy()
    df["source_name"] = nom_source

    # Trier pour prendre 1er arrêt = départ ; dernier = arrivée
    df_sorted = df.sort_values(["trip_id", "stop_sequence"])
    df_depart = df_sorted.groupby("trip_id").first().reset_index()
    df_arrivee = df_sorted.groupby("trip_id").last().reset_index()

    df_final = pd.DataFrame({
        "trip_id": df_depart["trip_id"],
        "source_name": nom_source,
        "dep_stop_name": df_depart["stop_name"],
        "departure_time": df_depart["departure_time"],
        "arr_stop_name": df_arrivee["stop_name"],
        "arrival_time": df_arrivee["arrival_time"],
    })

    # Qualité minimale
    df_final.dropna(subset=["dep_stop_name", "arr_stop_name", "departure_time", "arrival_time"], inplace=True)

    # Features analytiques
    df_final["is_night_train"] = df_final.apply(lambda r: compute_is_night(r["departure_time"], r["arrival_time"]), axis=1)

    dep_s = df_final["departure_time"].apply(parse_hhmmss_to_seconds)
    arr_s = df_final["arrival_time"].apply(parse_hhmmss_to_seconds)
    df_final["duration_seconds_est"] = (arr_s - dep_s)

    print(f"   ✅ {len(df_final)} trips résumés (départ/arrivée) pour {nom_source}")
    return df_final

# =========================================================
# 3) LOAD (PostgreSQL)
# =========================================================
def load_summary_to_postgres(engine, df_summary: pd.DataFrame, run_id: int) -> int:
    if df_summary.empty:
        return 0

    df_to_load = df_summary.copy()
    df_to_load["source_run_id"] = run_id

    # Astuce MSPR: pour rejouabilité, on peut éviter les doublons en supprimant d'abord les trip_id de cette source_run
    # Ici on fait simple: append. (On peut améliorer après)
    df_to_load.to_sql(
        "fact_trip_summary",
        engine,
        schema="obrail",
        if_exists="append",
        index=False,
        method="multi"
    )
    return len(df_to_load)

# =========================================================
# CSV SAVE (optionnel)
# =========================================================
def save_csv(df, path):
    if df.empty:
        print("⚠️ Pas de données à sauvegarder.")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"💾 Fichier CSV créé : {path}")

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 200)

    engine = create_engine(DB_URL)

    all_summaries = []
    total_loaded = 0

    for src in GTFS_SOURCES:
        run_id = start_etl_run(engine, src["nom"], src["url"])
        try:
            gtfs_data = download_gtfs(src["url"], src["nom"])
            df_summary = transform_gtfs_to_summary(gtfs_data, src["nom"])

            rows_loaded = load_summary_to_postgres(engine, df_summary, run_id)
            finish_etl_run(engine, run_id, "SUCCESS", len(df_summary), rows_loaded, None)

            if not df_summary.empty:
                all_summaries.append(df_summary)
                total_loaded += rows_loaded

        except Exception as e:
            finish_etl_run(engine, run_id, "FAILED", 0, 0, str(e))
            raise

    if all_summaries:
        df_all = pd.concat(all_summaries, ignore_index=True)
        save_csv(df_all, OUTPUT_FILE)

        print("\n--- Aperçu (10 lignes) ---")
        print(df_all.head(10))

        print(f"\n✅ Total chargé en BDD : {total_loaded}")
    else:
        print("⚠️ Aucune donnée transformée.")

    print("\n" + "=" * 60)
    print("FIN DU PROCESSUS ETL")
