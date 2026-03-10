import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests

# ==========================================
# CONFIGURATION DE LA PAGE
# ==========================================
st.set_page_config(
    page_title="ObRail Europe - Dashboard",
    page_icon="🚄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==========================================
# STYLES CSS PERSONNALISÉS
# ==========================================
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Global Styles */
    .stApp {
        background: linear-gradient(180deg, #0a0a0f 0%, #12121a 100%);
        font-family: 'Inter', sans-serif;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Custom Header */
    .main-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 16px;
        padding: 2.5rem;
        margin-bottom: 2rem;
        text-align: center;
        position: relative;
        overflow: hidden;
    }
    
    .main-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #6366f1, #22d3ee, #6366f1);
    }
    
    .main-header h1 {
        color: #ffffff;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        letter-spacing: -0.02em;
    }
    
    .main-header p {
        color: #94a3b8;
        font-size: 1.1rem;
        margin-bottom: 1.5rem;
    }
    
    .badge-container {
        display: flex;
        justify-content: center;
        gap: 0.75rem;
        flex-wrap: wrap;
    }
    
    .tech-badge {
        background: rgba(99, 102, 241, 0.15);
        border: 1px solid rgba(99, 102, 241, 0.3);
        color: #a5b4fc;
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 500;
    }
    
    /* Section Titles */
    .section-title {
        color: #ffffff;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(99, 102, 241, 0.3);
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    /* Stat Cards */
    .stat-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #252536 100%);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        transition: all 0.3s ease;
    }
    
    .stat-card:hover {
        border-color: rgba(99, 102, 241, 0.5);
        transform: translateY(-2px);
    }
    
    .stat-value {
        font-size: 2rem;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 0.25rem;
    }
    
    .stat-value.primary { color: #6366f1; }
    .stat-value.success { color: #22c55e; }
    .stat-value.warning { color: #f59e0b; }
    .stat-value.info { color: #22d3ee; }
    
    .stat-label {
        color: #94a3b8;
        font-size: 0.9rem;
        font-weight: 500;
    }
    
    /* API Endpoint Cards */
    .endpoint-card {
        background: #1e1e2e;
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }
    
    .endpoint-method {
        display: inline-block;
        background: rgba(34, 197, 94, 0.2);
        color: #22c55e;
        padding: 0.25rem 0.75rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 0.75rem;
    }
    
    .endpoint-path {
        color: #e2e8f0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.95rem;
    }
    
    .endpoint-desc {
        color: #64748b;
        font-size: 0.85rem;
        margin-top: 0.5rem;
    }
    
    /* ETL Status Table */
    .etl-status {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .etl-success {
        background: rgba(34, 197, 94, 0.2);
        color: #22c55e;
    }
    
    .etl-error {
        background: rgba(239, 68, 68, 0.2);
        color: #ef4444;
    }
    
    .etl-running {
        background: rgba(245, 158, 11, 0.2);
        color: #f59e0b;
    }
    
    /* Footer */
    .footer {
        background: #0f0f14;
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        padding: 2rem;
        margin-top: 3rem;
        text-align: center;
        border-radius: 16px 16px 0 0;
    }
    
    .footer-title {
        color: #ffffff;
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    
    .footer-text {
        color: #64748b;
        font-size: 0.9rem;
    }
    
    /* Navigation Pills */
    .nav-container {
        display: flex;
        justify-content: center;
        gap: 0.5rem;
        margin-bottom: 2rem;
        flex-wrap: wrap;
    }
    
    /* Custom Dataframe styling */
    .stDataFrame {
        background: #1e1e2e !important;
        border-radius: 10px !important;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: rgba(30, 30, 46, 0.8);
        border-radius: 8px;
        color: #94a3b8;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .stTabs [aria-selected="true"] {
        background: rgba(99, 102, 241, 0.2) !important;
        border-color: rgba(99, 102, 241, 0.5) !important;
        color: #ffffff !important;
    }
    
    /* Input fields */
    .stTextInput > div > div > input {
        background: #1e1e2e;
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        color: #ffffff;
    }
    
    .stSelectbox > div > div {
        background: #1e1e2e;
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #4f46e5 0%, #4338ca 100%);
        transform: translateY(-1px);
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 1.8rem !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }
    
    /* Cards container */
    .card-container {
        background: linear-gradient(135deg, #1e1e2e 0%, #252536 100%);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    
    .card-title {
        color: #ffffff;
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# CONFIGURATION API
# ==========================================
API_BASE_URL = "http://127.0.0.1:8080"

# ==========================================
# FONCTIONS UTILITAIRES
# ==========================================
@st.cache_data(ttl=60)
def fetch_etl_runs():
    """Récupère l'historique des runs ETL depuis l'API."""
    try:
        response = requests.get(f"{API_BASE_URL}/quality/etl-runs", timeout=5)
        if response.status_code == 200:
            return response.json().get("data", [])
    except:
        pass
    # Données de démonstration
    return [
        {"pipeline_name": "GTFS_France", "source_name": "SNCF", "status": "success", "rows_extracted": 45230, "rows_loaded": 45230, "finished_at": "2024-01-15 14:32:00"},
        {"pipeline_name": "GTFS_Germany", "source_name": "Deutsche Bahn", "status": "success", "rows_extracted": 38450, "rows_loaded": 38450, "finished_at": "2024-01-15 14:28:00"},
        {"pipeline_name": "NightJet", "source_name": "ÖBB", "status": "success", "rows_extracted": 12340, "rows_loaded": 12340, "finished_at": "2024-01-15 14:15:00"},
        {"pipeline_name": "Eurostar", "source_name": "Eurostar", "status": "running", "rows_extracted": 8500, "rows_loaded": 0, "finished_at": None},
        {"pipeline_name": "TGV_Lyria", "source_name": "SNCF", "status": "error", "rows_extracted": 5200, "rows_loaded": 0, "finished_at": "2024-01-15 13:45:00"},
    ]

@st.cache_data(ttl=60)
def fetch_kpis():
    """Récupère les KPIs depuis l'API."""
    try:
        response = requests.get(f"{API_BASE_URL}/kpis", timeout=5)
        if response.status_code == 200:
            return response.json().get("data", [])
    except:
        pass
    # Données de démonstration
    return [
        {"source": "SNCF", "category": "jour", "nb_rows": 28500},
        {"source": "SNCF", "category": "nuit", "nb_rows": 3200},
        {"source": "Deutsche Bahn", "category": "jour", "nb_rows": 22100},
        {"source": "Deutsche Bahn", "category": "nuit", "nb_rows": 1800},
        {"source": "ÖBB", "category": "jour", "nb_rows": 8900},
        {"source": "ÖBB", "category": "nuit", "nb_rows": 4500},
        {"source": "Trenitalia", "category": "jour", "nb_rows": 15600},
        {"source": "Eurostar", "category": "jour", "nb_rows": 6200},
        {"source": "Renfe", "category": "jour", "nb_rows": 9800},
        {"source": "Renfe", "category": "nuit", "nb_rows": 1200},
    ]

def search_trains(origine=None, destination=None, category=None):
    """Recherche des trains via l'API."""
    try:
        params = {}
        if origine: params["origine"] = origine
        if destination: params["destination"] = destination
        if category: params["category"] = category
        response = requests.get(f"{API_BASE_URL}/trains/search", params=params, timeout=5)
        if response.status_code == 200:
            return response.json().get("data", [])
    except:
        pass
    # Données de démonstration
    demo_data = [
        {"train_id": "TGV-8234", "source": "SNCF", "category": "jour", "operator": "SNCF Voyageurs", "origin_stop": "Paris Gare de Lyon", "origin_departure": "08:15", "destination_stop": "Lyon Part-Dieu", "destination_arrival": "10:12"},
        {"train_id": "ICE-592", "source": "Deutsche Bahn", "category": "jour", "operator": "DB Fernverkehr", "origin_stop": "Frankfurt Hbf", "origin_departure": "09:30", "destination_stop": "Berlin Hbf", "destination_arrival": "13:45"},
        {"train_id": "NJ-421", "source": "ÖBB", "category": "nuit", "operator": "ÖBB Nightjet", "origin_stop": "Vienna Hbf", "origin_departure": "21:10", "destination_stop": "Rome Termini", "destination_arrival": "09:35"},
        {"train_id": "EUR-9012", "source": "Eurostar", "category": "jour", "operator": "Eurostar", "origin_stop": "London St Pancras", "origin_departure": "07:01", "destination_stop": "Paris Gare du Nord", "destination_arrival": "10:17"},
        {"train_id": "AVE-3421", "source": "Renfe", "category": "jour", "operator": "Renfe AVE", "origin_stop": "Madrid Atocha", "origin_departure": "11:00", "destination_stop": "Barcelona Sants", "destination_arrival": "13:30"},
    ]
    
    results = demo_data
    if origine:
        results = [t for t in results if origine.lower() in t["origin_stop"].lower()]
    if destination:
        results = [t for t in results if destination.lower() in t["destination_stop"].lower()]
    if category:
        results = [t for t in results if t["category"] == category]
    return results

# ==========================================
# HEADER PRINCIPAL
# ==========================================
st.markdown("""
<div class="main-header">
    <h1>🚄 ObRail Europe</h1>
    <p>Plateforme de données ferroviaires européennes</p>
    <div class="badge-container">
        <span class="tech-badge">FastAPI</span>
        <span class="tech-badge">PostgreSQL</span>
        <span class="tech-badge">ETL Pipeline</span>
        <span class="tech-badge">GTFS Data</span>
        <span class="tech-badge">Streamlit</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ==========================================
# NAVIGATION PAR ONGLETS
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs(["📊 Vue d'ensemble", "⚙️ Qualité ETL", "🔍 Recherche Trains", "📡 API Documentation"])

# ==========================================
# TAB 1: VUE D'ENSEMBLE
# ==========================================
with tab1:
    st.markdown('<h2 class="section-title">📈 Statistiques Globales</h2>', unsafe_allow_html=True)
    
    # Calcul des stats depuis les KPIs
    kpis = fetch_kpis()
    total_rows = sum(item.get("nb_rows", 0) for item in kpis)
    sources = list(set(item.get("source", "") for item in kpis))
    categories = list(set(item.get("category", "") for item in kpis))
    
    # Cartes de statistiques
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value primary">{total_rows:,}</div>
            <div class="stat-label">Trajets Ferroviaires</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value success">{len(sources)}</div>
            <div class="stat-label">Sources de Données</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value warning">{len(categories)}</div>
            <div class="stat-label">Catégories</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        etl_runs = fetch_etl_runs()
        # CORRECTION ICI : On cherche "SUCCESS" en majuscules
        success_rate = len([r for r in etl_runs if r.get("status") == "SUCCESS"]) / max(len(etl_runs), 1) * 100
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value info">{success_rate:.0f}%</div>
            <div class="stat-label">Taux de Succès ETL</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Graphiques
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.markdown('<div class="card-container"><div class="card-title">📊 Répartition par Source</div>', unsafe_allow_html=True)
        df_kpis = pd.DataFrame(kpis)
        if not df_kpis.empty:
            df_grouped = df_kpis.groupby("source")["nb_rows"].sum().reset_index()
            
            # NOUVEAUTÉ : On crée une légende personnalisée avec les pourcentages
            total_camembert = df_grouped["nb_rows"].sum()
            df_grouped["legende"] = df_grouped.apply(
                lambda row: f"{row['source']} ({(row['nb_rows']/total_camembert)*100:.3f}%)", axis=1
            )
            
            fig = px.pie(
                df_grouped, 
                values="nb_rows", 
                names="legende", # On utilise la nouvelle colonne qui contient le pourcentage
                color_discrete_sequence=["#6366f1", "#22c55e", "#f59e0b", "#ef4444", "#22d3ee", "#a855f7"],
                hole=0.4
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#94a3b8",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.3), # Descendu légèrement
                margin=dict(t=20, b=40, l=20, r=20),
                height=320
            )
            # On masque le texte sur le graphique lui-même pour ne pas le surcharger, tout est dans la légende !
            fig.update_traces(textinfo='none', hoverinfo='label+value')
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col_chart2:
        st.markdown('<div class="card-container"><div class="card-title">🌙 Jour vs Nuit</div>', unsafe_allow_html=True)
        if not df_kpis.empty:
            df_cat = df_kpis.groupby("category")["nb_rows"].sum().reset_index()
            fig2 = px.bar(
                df_cat,
                x="category",
                y="nb_rows",
                color="category",
                color_discrete_map={"jour": "#6366f1", "nuit": "#22d3ee"},
                text="nb_rows"
            )
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#94a3b8",
                showlegend=False,
                xaxis=dict(title="", gridcolor="rgba(255,255,255,0.1)"),
                yaxis=dict(title="Nombre de trajets", gridcolor="rgba(255,255,255,0.1)"),
                margin=dict(t=20, b=20, l=20, r=20),
                height=300
            )
            fig2.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig2, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# TAB 2: QUALITÉ ETL
# ==========================================
with tab2:
    st.markdown('<h2 class="section-title">⚙️ État des Pipelines ETL</h2>', unsafe_allow_html=True)
    
    etl_data = fetch_etl_runs()
    
    # Métriques rapides - CORRECTION DES MAJUSCULES ICI
    col1, col2, col3 = st.columns(3)
    success_count = len([r for r in etl_data if r.get("status") == "SUCCESS"])
    error_count = len([r for r in etl_data if r.get("status") == "FAILED"])
    running_count = len([r for r in etl_data if r.get("status") == "RUNNING"])
    
    with col1:
        st.metric("Pipelines Réussis", success_count, delta="Stable")
    with col2:
        st.metric("En Erreur", error_count, delta="-1" if error_count > 0 else "0")
    with col3:
        st.metric("En Cours", running_count)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Tableau des runs
    st.markdown('<div class="card-container"><div class="card-title">📋 Historique des Exécutions</div>', unsafe_allow_html=True)
    
    if etl_data:
        df_etl = pd.DataFrame(etl_data)
        
        # Formatage du statut avec couleurs - CORRECTION DES MAJUSCULES ICI
        def style_status(val):
            if val == "SUCCESS":
                return "background: rgba(34, 197, 94, 0.2); color: #22c55e; padding: 4px 12px; border-radius: 12px;"
            elif val == "FAILED":
                return "background: rgba(239, 68, 68, 0.2); color: #ef4444; padding: 4px 12px; border-radius: 12px;"
            else:
                return "background: rgba(245, 158, 11, 0.2); color: #f59e0b; padding: 4px 12px; border-radius: 12px;"
        
        # Application du style si tu utilises pandas styler, sinon l'affichage basique
        # Affichage du tableau
        st.dataframe(
            df_etl[["pipeline_name", "source_name", "status", "rows_extracted", "rows_loaded", "finished_at"]],
            column_config={
                "pipeline_name": st.column_config.TextColumn("Pipeline", width="medium"),
                "source_name": st.column_config.TextColumn("Source", width="medium"),
                "status": st.column_config.TextColumn("Statut", width="small"),
                "rows_extracted": st.column_config.NumberColumn("Lignes Extraites", format="%d"),
                "rows_loaded": st.column_config.NumberColumn("Lignes Chargées", format="%d"),
                "finished_at": st.column_config.TextColumn("Terminé le", width="medium"),
            },
            hide_index=True,
            use_container_width=True
        )
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Graphique de performance
    st.markdown('<div class="card-container"><div class="card-title">📈 Performance des Pipelines</div>', unsafe_allow_html=True)
    if etl_data:
        df_perf = pd.DataFrame(etl_data)
        # CORRECTION DE LA MAJUSCULE ICI AUSSI
        df_perf = df_perf[df_perf["status"] == "SUCCESS"]
        if not df_perf.empty:
            fig3 = px.bar(
                df_perf,
                x="pipeline_name",
                y="rows_loaded",
                color="source_name",
                color_discrete_sequence=["#6366f1", "#22c55e", "#f59e0b", "#22d3ee"],
                text="rows_loaded"
            )
            fig3.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#94a3b8",
                xaxis=dict(title="", gridcolor="rgba(255,255,255,0.1)"),
                yaxis=dict(title="Lignes chargées", gridcolor="rgba(255,255,255,0.1)"),
                legend=dict(orientation="h", yanchor="bottom", y=-0.3),
                margin=dict(t=20, b=60, l=20, r=20),
                height=350
            )
            fig3.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig3, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# TAB 3: RECHERCHE TRAINS
# ==========================================
with tab3:
    st.markdown('<h2 class="section-title">🔍 Recherche de Trajets</h2>', unsafe_allow_html=True)
    
    # Formulaire de recherche
    st.markdown('<div class="card-container"><div class="card-title">Critères de Recherche</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        origine = st.text_input("Ville de départ", placeholder="Ex: Paris, Berlin, Vienna...")
    with col2:
        destination = st.text_input("Ville d'arrivée", placeholder="Ex: Lyon, Rome, Madrid...")
    with col3:
        category = st.selectbox("Type de train", ["Tous", "jour", "nuit"])
    
    search_btn = st.button("🔍 Rechercher", type="primary", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Résultats
    if search_btn or origine or destination:
        cat_filter = None if category == "Tous" else category
        results = search_trains(origine or None, destination or None, cat_filter)
        
        st.markdown(f'<h3 class="section-title">🚆 Résultats ({len(results)} trajets trouvés)</h3>', unsafe_allow_html=True)
        
        if results:
            for train in results:
                category_badge = "🌞 Jour" if train.get("category") == "jour" else "🌙 Nuit"
                category_color = "#6366f1" if train.get("category") == "jour" else "#22d3ee"
                
                st.markdown(f"""
                <div class="card-container" style="border-left: 4px solid {category_color};">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 1rem;">
                        <div>
                            <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem;">
                                <span style="font-size: 1.25rem; font-weight: 700; color: #ffffff;">{train.get('train_id', 'N/A')}</span>
                                <span style="background: rgba(99, 102, 241, 0.2); color: #a5b4fc; padding: 0.25rem 0.75rem; border-radius: 12px; font-size: 0.8rem;">{train.get('operator', 'N/A')}</span>
                                <span style="background: {'rgba(99, 102, 241, 0.2)' if train.get('category') == 'jour' else 'rgba(34, 211, 238, 0.2)'}; color: {category_color}; padding: 0.25rem 0.75rem; border-radius: 12px; font-size: 0.8rem;">{category_badge}</span>
                            </div>
                            <div style="color: #94a3b8; font-size: 0.9rem;">Source: {train.get('source', 'N/A')}</div>
                        </div>
                        <div style="display: flex; align-items: center; gap: 2rem;">
                            <div style="text-align: center;">
                                <div style="font-size: 1.5rem; font-weight: 700; color: #ffffff;">{train.get('origin_departure', '--:--')}</div>
                                <div style="color: #94a3b8; font-size: 0.85rem; max-width: 150px;">{train.get('origin_stop', 'N/A')}</div>
                            </div>
                            <div style="color: #6366f1; font-size: 1.5rem;">→</div>
                            <div style="text-align: center;">
                                <div style="font-size: 1.5rem; font-weight: 700; color: #ffffff;">{train.get('destination_arrival', '--:--')}</div>
                                <div style="color: #94a3b8; font-size: 0.85rem; max-width: 150px;">{train.get('destination_stop', 'N/A')}</div>
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Aucun trajet trouvé avec ces critères. Essayez d'élargir votre recherche.")

# ==========================================
# TAB 4: API DOCUMENTATION
# ==========================================
with tab4:
    st.markdown('<h2 class="section-title">📡 Documentation API</h2>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="card-container">
        <div class="card-title">🔗 Base URL</div>
        <code style="background: #1a1a2e; padding: 0.75rem 1rem; border-radius: 8px; color: #22d3ee; display: block; font-size: 1rem;">
            http://127.0.0.1:8080
        </code>
    </div>
    """, unsafe_allow_html=True)
    
    # Endpoints
    endpoints = [
        {
            "method": "GET",
            "path": "/quality/etl-runs",
            "description": "Récupère l'historique des derniers lancements de l'ETL avec statut, nombre de lignes extraites/chargées.",
            "tag": "Qualité des Données"
        },
        {
            "method": "GET",
            "path": "/kpis",
            "description": "Récupère la répartition globale des données par source et catégorie.",
            "tag": "Statistiques"
        },
        {
            "method": "GET",
            "path": "/trains/search",
            "description": "Recherche des trajets ferroviaires selon des critères (origine, destination, catégorie).",
            "tag": "Recherche",
            "params": ["origine", "destination", "category", "limit"]
        }
    ]
    
    for ep in endpoints:
        params_html = ""
        if ep.get("params"):
            params_html = f"<div style='color: #64748b; font-size: 0.8rem; margin-top: 0.5rem;'>Paramètres: <code style='color: #a5b4fc;'>{', '.join(ep['params'])}</code></div>"
        
        st.markdown(f"""
        <div class="endpoint-card">
            <div style="display: flex; align-items: center; flex-wrap: wrap; gap: 0.5rem;">
                <span class="endpoint-method">{ep['method']}</span>
                <span class="endpoint-path">{ep['path']}</span>
                <span style="background: rgba(99, 102, 241, 0.15); color: #a5b4fc; padding: 0.2rem 0.6rem; border-radius: 6px; font-size: 0.7rem; margin-left: auto;">{ep['tag']}</span>
            </div>
            <div class="endpoint-desc">{ep['description']}</div>
            {params_html}
        </div>
        """, unsafe_allow_html=True)
    
    # Architecture
    st.markdown('<div class="card-container"><div class="card-title">🏗️ Architecture du Projet</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Stack Technique**
        - **API Backend:** FastAPI + Uvicorn
        - **Base de données:** PostgreSQL
        - **ORM:** SQLAlchemy
        - **Dashboard:** Streamlit + Plotly
        """)
    
    with col2:
        st.markdown("""
        **Pipeline ETL**
        - **Sources:** GTFS, APIs ferroviaires
        - **Traitement:** Python + Pandas
        - **Stockage:** PostgreSQL (schéma obrail)
        - **Monitoring:** Logs ETL en temps réel
        """)
    
    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# FOOTER
# ==========================================
st.markdown("""
<div class="footer">
    <div class="footer-title">ObRail Europe - Projet MSPR</div>
    <div class="footer-text">
        Bloc 6.1 - Mise en place d'une solution de gestion de données<br>
        Développé avec FastAPI, PostgreSQL, et Streamlit
    </div>
</div>
""", unsafe_allow_html=True)
