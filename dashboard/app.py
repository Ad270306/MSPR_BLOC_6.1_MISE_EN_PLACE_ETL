import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="ObRail – Trains Europe",
    layout="wide",
    page_icon="🚆"
)

load_dotenv()
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/obrail")
engine = create_engine(DB_URL)

# =========================
# LOAD DATA (simplifié)
# =========================
@st.cache_data(ttl=3600)
def load_data():
    query = """
        SELECT 
            train_id,
            source,
            category,
            operator,
            stop_name,
            departure_time,
            arrival_time,
            countries,
            loaded_at
        FROM obrail.fact_train_stop
        ORDER BY random()  -- Pour avoir un échantillon varié
        LIMIT 5000  -- Limite pour la performance
    """
    df = pd.read_sql(query, engine)
    
    # Nettoie les données
    df['countries'] = df['countries'].fillna('Inconnu')
    df['countries'] = df['countries'].replace('nan', 'Inconnu')
    df['operator'] = df['operator'].fillna('Inconnu')
    
    # Extrait l'heure (pour les graphiques)
    df['heure'] = pd.to_datetime(df['departure_time'], errors='coerce').dt.hour
    
    # Simplifie les pays (prend le premier pays si plusieurs)
    df['pays_principal'] = df['countries'].apply(lambda x: x.split(',')[0].strip() if x != 'Inconnu' else 'Inconnu')
    
    return df

df = load_data()

# =========================
# TITRE
# =========================
st.title("🚆 ObRail – Trains en Europe")
st.markdown("#### Visualisation simple des trains, horaires et pays")

# =========================
# FILTRES SUPER SIMPLES
# =========================
with st.sidebar:
    st.header("🔎 Filtres")
    
    # Boutons radio pour le type de train
    type_filter = st.radio(
        "Type de train",
        ["Tous", "Jour", "Nuit"],
        horizontal=True
    )
    
    # Liste déroulante pour le pays
    pays_list = ['Tous'] + sorted(df['pays_principal'].unique().tolist())
    pays_filter = st.selectbox("Pays", pays_list)
    
    # Slider pour l'heure
    heure_filter = st.slider("Heure de départ", 0, 23, (0, 23))
    
    # Liste déroulante pour l'opérateur
    operateurs = ['Tous'] + sorted(df['operator'].unique().tolist())
    operateur_filter = st.selectbox("Opérateur", operateurs)

# Application des filtres
df_filtered = df.copy()

if type_filter == "Jour":
    df_filtered = df_filtered[df_filtered['category'] == 'jour']
elif type_filter == "Nuit":
    df_filtered = df_filtered[df_filtered['category'] == 'nuit']

if pays_filter != 'Tous':
    df_filtered = df_filtered[df_filtered['pays_principal'] == pays_filter]

if operateur_filter != 'Tous':
    df_filtered = df_filtered[df_filtered['operator'] == operateur_filter]

df_filtered = df_filtered[
    (df_filtered['heure'] >= heure_filter[0]) & 
    (df_filtered['heure'] <= heure_filter[1])
]

# =========================
# GRANDS CHIFFRES (KPI)
# =========================
col1, col2, col3, col4 = st.columns(4)

with col1:
    nb_trains = df_filtered['train_id'].nunique()
    st.metric("🚆 Trains", nb_trains)

with col2:
    nb_gares = df_filtered['stop_name'].nunique()
    st.metric("🏢 Gares", nb_gares)

with col3:
    nb_pays = df_filtered['pays_principal'].nunique()
    st.metric("🌍 Pays", nb_pays)

with col4:
    nb_operateurs = df_filtered['operator'].nunique()
    st.metric("🏭 Opérateurs", nb_operateurs)

st.divider()

# =========================
# PARTIE 1: PAYS ET OPÉRATEURS (SIMPLE)
# =========================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🌍 Trains par pays")
    
    # Compte les trains par pays
    pays_counts = df_filtered.groupby('pays_principal').size().reset_index(name='count')
    pays_counts = pays_counts.sort_values('count', ascending=False).head(10)
    
    if not pays_counts.empty:
        fig = px.bar(
            pays_counts,
            x='pays_principal',
            y='count',
            color='count',
            color_continuous_scale='Viridis',
            labels={'pays_principal': 'Pays', 'count': 'Nombre de trains'}
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée pays")

with col2:
    st.subheader("🏭 Trains par opérateur")
    
    # Compte les trains par opérateur
    op_counts = df_filtered.groupby('operator').size().reset_index(name='count')
    op_counts = op_counts.sort_values('count', ascending=False).head(10)
    
    if not op_counts.empty:
        fig = px.bar(
            op_counts,
            x='operator',
            y='count',
            color='count',
            color_continuous_scale='Plasma',
            labels={'operator': 'Opérateur', 'count': 'Nombre de trains'}
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée opérateur")

st.divider()

# =========================
# PARTIE 2: HORAIRES (TRÈS SIMPLE)
# =========================
st.subheader("🕐 Horaires des départs")

col1, col2 = st.columns(2)

with col1:
    # Graphique en barres des heures
    heure_counts = df_filtered['heure'].value_counts().sort_index().reset_index()
    heure_counts.columns = ['Heure', 'Nombre']
    
    if not heure_counts.empty:
        fig = px.bar(
            heure_counts,
            x='Heure',
            y='Nombre',
            title="Nombre de départs par heure",
            color='Nombre',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée horaire")

with col2:
    # Répartition Jour/Nuit
    jour_nuit = df_filtered['category'].value_counts().reset_index()
    jour_nuit.columns = ['Type', 'Nombre']
    jour_nuit['Type'] = jour_nuit['Type'].map({'jour': 'Jour ☀️', 'nuit': 'Nuit 🌙'})
    
    if not jour_nuit.empty:
        fig = px.pie(
            jour_nuit,
            values='Nombre',
            names='Type',
            title="Répartition Jour / Nuit",
            color='Type',
            color_discrete_map={'Jour ☀️': '#FFD700', 'Nuit 🌙': '#2C3E50'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée")

st.divider()

# =========================
# PARTIE 3: RECHERCHE SIMPLE
# =========================
st.subheader("🔍 Recherche rapide")

col1, col2 = st.columns(2)

with col1:
    # Recherche par gare
    gare = st.selectbox("Voir les trains d'une gare", [''] + sorted(df_filtered['stop_name'].unique().tolist()))
    
    if gare:
        trains_gare = df_filtered[df_filtered['stop_name'] == gare]
        st.write(f"🚆 {len(trains_gare)} trains trouvés")
        
        # Affiche un petit tableau
        st.dataframe(
            trains_gare[['train_id', 'operator', 'departure_time', 'pays_principal']].head(10),
            use_container_width=True,
            hide_index=True,
            column_config={
                'train_id': 'Train',
                'operator': 'Opérateur',
                'departure_time': 'Départ',
                'pays_principal': 'Pays'
            }
        )

with col2:
    # Recherche par opérateur
    op = st.selectbox("Voir les trains d'un opérateur", [''] + sorted(df_filtered['operator'].unique().tolist()))
    
    if op:
        trains_op = df_filtered[df_filtered['operator'] == op]
        st.write(f"🚆 {len(trains_op)} trains trouvés")
        
        # Affiche un petit tableau
        st.dataframe(
            trains_op[['train_id', 'stop_name', 'departure_time', 'pays_principal']].head(10),
            use_container_width=True,
            hide_index=True,
            column_config={
                'train_id': 'Train',
                'stop_name': 'Gare',
                'departure_time': 'Départ',
                'pays_principal': 'Pays'
            }
        )

st.divider()

# =========================
# PARTIE 4: APERÇU SIMPLE DES DONNÉES
# =========================
st.subheader("📋 Aperçu des trains")

# Tableau simple avec les infos essentielles
apercu = df_filtered[['train_id', 'operator', 'stop_name', 'departure_time', 'pays_principal']].copy()
apercu.columns = ['Train', 'Opérateur', 'Gare', 'Départ', 'Pays']
apercu = apercu.drop_duplicates().head(100)  # Limite pour la performance

st.dataframe(
    apercu,
    use_container_width=True,
    hide_index=True,
    height=300
)

# =========================
# STATISTIQUES RAPIDES (OPTIONNEL)
# =========================
with st.expander("📊 Voir plus de statistiques"):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Top 5 pays**")
        top_pays = df_filtered['pays_principal'].value_counts().head(5)
        for pays, count in top_pays.items():
            st.write(f"- {pays}: {count} trains")
    
    with col2:
        st.write("**Top 5 opérateurs**")
        top_ops = df_filtered['operator'].value_counts().head(5)
        for op, count in top_ops.items():
            st.write(f"- {op}: {count} trains")
    
    with col3:
        st.write("**Heures de pointe**")
        heures = df_filtered['heure'].value_counts().head(3)
        for h, count in heures.items():
            st.write(f"- {int(h)}h: {count} départs")

# =========================
# FOOTER
# =========================
st.divider()
st.caption(f"Mise à jour: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}")