import uvicorn
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text

# ==========================================
# CONFIGURATION DB (Directe et codée en dur)
# ==========================================
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/obrail"
engine = create_engine(DB_URL)

app = FastAPI(
    title="ObRail Europe API",
    description="",
    version="1.0.0"
)

# ==========================================
# ROUTES : QUALITÉ DES DONNÉES (Dashboard)
# ==========================================
@app.get("/quality/etl-runs", tags=["Qualité des Données"])
def get_etl_status():
    """Récupère l'historique des derniers lancements de l'ETL."""
    query = text("""
        SELECT pipeline_name, source_name, status, rows_extracted, rows_loaded, finished_at
        FROM obrail.etl_run
        ORDER BY finished_at DESC
        LIMIT 10;
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query).mappings().all()
            return {"data": [dict(row) for row in result]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/kpis", tags=["Statistiques"])
def get_kpis():
    """Récupère la répartition globale des données."""
    query = text("""
        SELECT source, category, COUNT(*) AS nb_rows
        FROM obrail.fact_train_stop
        GROUP BY source, category
        ORDER BY source, category;
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query).mappings().all()
            return {"data": [dict(row) for row in result]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ROUTES : RECHERCHE DE TRAINS (Cahier des charges)
# ==========================================
@app.get("/trains/search", tags=["Recherche"])
def search_trains(
    origine: str = Query(None, description="Ville de départ (ex: Paris)"),
    destination: str = Query(None, description="Ville d'arrivée (ex: Berlin)"),
    category: str = Query(None, description="Type de train ('jour' ou 'nuit')"),
    limit: int = Query(50, description="Nombre maximum de résultats")
):
    """Recherche des trajets ferroviaires selon des critères précis."""
    base_query = """
        SELECT train_id, source, category, operator, origin_stop, origin_departure, destination_stop, destination_arrival
        FROM obrail.v_train_summary
        WHERE 1=1
    """
    params = {"limit": limit}

    if origine:
        base_query += " AND origin_stop ILIKE :origine"
        params["origine"] = f"%{origine}%"
    if destination:
        base_query += " AND destination_stop ILIKE :destination"
        params["destination"] = f"%{destination}%"
    if category:
        base_query += " AND category = :category"
        params["category"] = category
        
    base_query += " LIMIT :limit;"

    try:
        with engine.connect() as conn:
            result = conn.execute(text(base_query), params).mappings().all()
            return {"data": [dict(row) for row in result], "total_returned": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)