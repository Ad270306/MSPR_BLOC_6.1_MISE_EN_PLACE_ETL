import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================================================
# PATHS (ta structure actuelle)
# =========================================================
DAY_FILE = "data/raw/trains_europe_clean.csv"
NIGHT_FILE = "data/raw/Night Train Database.csv"
GTFS_FOLDER = "data/raw/gtfs_europe"
OUTPUT_FILE = "data/processed/trains_europe_final_clean.csv"

PIPELINE_NAME = "etl_train_stop_level"

# =========================================================
# DB
# =========================================================
load_dotenv()
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/obrail")

# =========================================================
# ETL SOURCES (3)
# =========================================================
SOURCES = [
    {"name": "SNCF_France", "type": "DAY_CSV", "ref": DAY_FILE},
    {"name": "BackOnTrack", "type": "NIGHT_CSV", "ref": NIGHT_FILE},
    {"name": "GTFS_Europe", "type": "GTFS_FOLDER", "ref": GTFS_FOLDER},
]

# =========================================================
# Helpers
# =========================================================
def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def strip_html(s: str) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    return re.sub(r"<.*?>", "", str(s))

# =========================================================
# ETL RUN LOG
# =========================================================
def start_etl_run(engine, pipeline_name: str, source_name: str, source_url: str) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                INSERT INTO obrail.etl_run(pipeline_name, source_name, source_url, status)
                VALUES (:p, :n, :u, 'RUNNING')
                RETURNING run_id
            """),
            {"p": pipeline_name, "n": source_name, "u": source_url}
        )
        return res.scalar()

def finish_etl_run(engine, run_id: int, status: str, rows_extracted: int, rows_loaded: int, error_message: str | None = None):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE obrail.etl_run
                SET finished_at = now(),
                    status = :st,
                    rows_extracted = :re,
                    rows_loaded = :rl,
                    error_message = :err
                WHERE run_id = :id
            """),
            {"st": status, "re": rows_extracted, "rl": rows_loaded, "err": error_message, "id": run_id}
        )

# =========================================================
# LOAD 1: trains de jour (France) -> stop-level minimal
# =========================================================
def load_day_trains() -> pd.DataFrame:
    print("🚆 Chargement trains de jour (France)...")
    df = pd.read_csv(DAY_FILE)
    df.columns = df.columns.str.strip()

    # train_id
    if "trip_id" in df.columns:
        train_id = df["trip_id"].astype(str)
    elif "train_id" in df.columns:
        train_id = df["train_id"].astype(str)
    else:
        train_id = pd.Series(range(len(df))).astype(str)

    # stop_name
    stop_name = None
    for c in ["stop_name", "origin", "dep_stop_name", "route_long_name", "name"]:
        if c in df.columns:
            stop_name = df[c].astype(str)
            break
    if stop_name is None:
        stop_name = pd.Series(["UNKNOWN"] * len(df))

    out = pd.DataFrame({
        "train_id": train_id,
        "source": "SNCF_France",
        "category": "jour",
        "operator": df["operator"].astype(str) if "operator" in df.columns else "SNCF",
        "stop_sequence": df["stop_sequence"] if "stop_sequence" in df.columns else 1,
        "stop_name": stop_name,
        "arrival_time": df["arrival_time"] if "arrival_time" in df.columns else None,
        "departure_time": df["departure_time"] if "departure_time" in df.columns else None,
        "route": None,
        "details": None,
        "tickets_url": None,
        "countries": None
    })

    out.drop_duplicates(inplace=True)
    print(f"   ✅ {len(out)} lignes (France)")
    return out

# =========================================================
# LOAD 2: trains de nuit (BackOnTrack) -> stop-level
# =========================================================
def load_night_trains() -> pd.DataFrame:
    print("🌙 Chargement trains de nuit (BackOnTrack)...")
    df = pd.read_csv(NIGHT_FILE)
    df.columns = df.columns.str.strip()

    # normalisation colonnes si fichier brut
    if len(df.columns) >= 8 and "train_code" not in df.columns:
        df = df.iloc[:, :8]
        df.columns = [
            "train_code", "name", "itinerary",
            "details_html", "route_html",
            "countries", "operator", "tickets_html"
        ]

    # si ces colonnes n’existent pas, on sécurise
    if "details_html" in df.columns:
        df["details"] = df["details_html"].apply(strip_html)
    else:
        df["details"] = None

    if "route_html" in df.columns:
        df["route"] = df["route_html"].apply(strip_html)
    else:
        df["route"] = None

    if "tickets_html" in df.columns:
        df["tickets_url"] = df["tickets_html"].astype(str).str.extract(r'href="([^"]+)"')[0]
    else:
        df["tickets_url"] = None

    out = pd.DataFrame({
        "train_id": df["train_code"].astype(str) if "train_code" in df.columns else pd.Series(range(len(df))).astype(str),
        "source": "BackOnTrack",
        "category": "nuit",
        "operator": df["operator"].astype(str) if "operator" in df.columns else None,
        "stop_sequence": 1,
        "stop_name": df["itinerary"].astype(str) if "itinerary" in df.columns else (df["name"].astype(str) if "name" in df.columns else "UNKNOWN"),
        "arrival_time": None,
        "departure_time": None,
        "route": df["route"],
        "details": df["details"],
        "tickets_url": df["tickets_url"],
        "countries": df["countries"].astype(str) if "countries" in df.columns else None
    })

    out.drop_duplicates(inplace=True)
    print(f"   ✅ {len(out)} lignes (Night DB)")
    return out

# =========================================================
# LOAD 3: GTFS Europe -> stop-level complet
# =========================================================
def load_european_trains() -> pd.DataFrame:
    print("🇪🇺 Chargement GTFS Europe (CD+13)...")

    routes = pd.read_csv(os.path.join(GTFS_FOLDER, "routes.txt"))
    trips = pd.read_csv(os.path.join(GTFS_FOLDER, "trips.txt"))
    stops = pd.read_csv(os.path.join(GTFS_FOLDER, "stops.txt"))
    stop_times = pd.read_csv(os.path.join(GTFS_FOLDER, "stop_times.txt"))

    for d in [routes, trips, stops, stop_times]:
        d.columns = d.columns.str.strip()

    trains_routes = routes[routes["route_type"].isin([2, 3])].copy()
    t = trains_routes.merge(trips, on="route_id", how="left")

    st = stop_times.merge(stops[["stop_id", "stop_name"]], on="stop_id", how="left")
    st = st[["trip_id", "arrival_time", "departure_time", "stop_sequence", "stop_name"]]

    df = t.merge(st, on="trip_id", how="left")

    operator = df["agency_id"].astype(str) if "agency_id" in df.columns else "UNKNOWN"

    out = pd.DataFrame({
        "train_id": df["trip_id"].astype(str),
        "source": "GTFS_Europe",
        "category": "jour",
        "operator": operator,
        "stop_sequence": df["stop_sequence"],
        "stop_name": df["stop_name"].astype(str),
        "arrival_time": df["arrival_time"],
        "departure_time": df["departure_time"],
        "route": df["route_long_name"].astype(str) if "route_long_name" in df.columns else None,
        "details": None,
        "tickets_url": None,
        "countries": None
    })

    out.dropna(subset=["train_id"], inplace=True)
    out.drop_duplicates(inplace=True)
    print(f"   ✅ {len(out)} lignes (GTFS Europe)")
    return out

# =========================================================
# MERGE + CLEAN
# =========================================================
def clean_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "stop_sequence" in df.columns:
        df["stop_sequence"] = pd.to_numeric(df["stop_sequence"], errors="coerce").astype("Int64")

    # strings
    for c in ["train_id", "source", "category", "operator", "stop_name"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df

# =========================================================
# LOAD PostgreSQL (batch + upsert)
# =========================================================
def load_to_postgres(engine, df: pd.DataFrame, run_id: int) -> int:
    df = df.copy()
    df["source_run_id"] = run_id

    sql = text("""
        INSERT INTO obrail.fact_train_stop(
            train_id, source, category, operator,
            stop_sequence, stop_name, arrival_time, departure_time,
            route, details, tickets_url, countries,
            source_run_id
        )
        VALUES (
            :train_id, :source, :category, :operator,
            :stop_sequence, :stop_name, :arrival_time, :departure_time,
            :route, :details, :tickets_url, :countries,
            :source_run_id
        )
        ON CONFLICT (train_id, source, stop_sequence, stop_name) DO UPDATE
        SET
            category = EXCLUDED.category,
            operator = EXCLUDED.operator,
            arrival_time = EXCLUDED.arrival_time,
            departure_time = EXCLUDED.departure_time,
            route = EXCLUDED.route,
            details = EXCLUDED.details,
            tickets_url = EXCLUDED.tickets_url,
            countries = EXCLUDED.countries,
            source_run_id = EXCLUDED.source_run_id,
            loaded_at = now();
    """)

    data = df.to_dict(orient="records")
    rows = 0

    with engine.begin() as conn:
        for chunk_start in range(0, len(data), 5000):
            chunk = data[chunk_start:chunk_start + 5000]
            conn.execute(sql, chunk)
            rows += len(chunk)

    return rows

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    print("============================================================")
    print("START ETL (3 sources -> processed CSV + PostgreSQL)")
    print("============================================================")

    engine = create_engine(DB_URL)

    all_frames = []
    total_loaded = 0
    total_extracted = 0

    for src in SOURCES:
        # 1 run par source
        run_id = start_etl_run(engine, PIPELINE_NAME, src["name"], src["ref"])

        try:
            if src["type"] == "DAY_CSV":
                df_part = load_day_trains()
            elif src["type"] == "NIGHT_CSV":
                df_part = load_night_trains()
            elif src["type"] == "GTFS_FOLDER":
                df_part = load_european_trains()
            else:
                df_part = pd.DataFrame()

            df_part = clean_types(df_part)

            rows_extracted = len(df_part)
            rows_loaded = load_to_postgres(engine, df_part, run_id)

            finish_etl_run(engine, run_id, "SUCCESS", rows_extracted, rows_loaded, None)

            all_frames.append(df_part)
            total_extracted += rows_extracted
            total_loaded += rows_loaded

            print(f"✅ Source {src['name']} -> extracted={rows_extracted}, loaded={rows_loaded}, run_id={run_id}")

        except Exception as e:
            finish_etl_run(engine, run_id, "FAILED", 0, 0, str(e))
            raise

    # CSV final (concat)
    if all_frames:
        df_final = pd.concat(all_frames, ignore_index=True).drop_duplicates()
        ensure_dir(OUTPUT_FILE)
        df_final.to_csv(OUTPUT_FILE, index=False)
        print(f"\n💾 CSV final créé: {OUTPUT_FILE} ({len(df_final)} lignes)")
    else:
        print("\n⚠️ Aucune donnée à écrire dans le CSV final.")

    print("\n============================================================")
    print(f"FIN ETL -> extracted={total_extracted} / loaded={total_loaded}")
    print("============================================================")