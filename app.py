import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

engine = create_engine('sqlite:///football_db.db')

st.set_page_config(page_title="Premier League Dashboard", layout="wide")

st.title("Premier League Analytics Dashboard")

# Get teams
df_teams = pd.read_sql("SELECT DISTINCT nomequipe FROM equipe", engine)
teams = ['All Teams'] + sorted(df_teams['nomequipe'].tolist())

selected_team = st.sidebar.selectbox("Select Team", teams)

analysis_types = [
    "Top 10 des meilleurs buteurs",
    "Joueurs les plus décisifs",
    "Joueurs les plus disciplinés (moins de cartons)",
    "Répartition des nationalités par équipe",
    "Nombre total de buts par équipe",
    "Moyenne de buts marqués/encaissés par match",
    "Classement des équipes",
    "Meilleures défenses (moins de buts concédés)",
    "Meilleurs buteurs par équipe",
    "Nombre total de matchs joués par équipe"
]

selected_analysis = st.sidebar.selectbox("Select Analysis", analysis_types)

queries = {
    "Top 10 des meilleurs buteurs": """
SELECT j.nomjoueur, j.position, j.nationalite, e.nomequipe,
       SUM(sj.buts) as total_buts
FROM joueur j
JOIN statistiquejoueur sj ON j.idjoueur = sj.idjoueur
JOIN equipe e ON j.id_equipe = e.idequipe
{excl}
GROUP BY j.idjoueur
ORDER BY total_buts DESC
LIMIT 10
""",
    "Joueurs les plus décisifs": """
SELECT j.nomjoueur, j.position, j.nationalite, e.nomequipe,
       SUM(sj.buts + sj.passesdecisives) as total_decisif
FROM joueur j
JOIN statistiquejoueur sj ON j.idjoueur = sj.idjoueur
JOIN equipe e ON j.id_equipe = e.idequipe
{excl}
GROUP BY j.idjoueur
ORDER BY total_decisif DESC
LIMIT 10
""",
    "Joueurs les plus disciplinés (moins de cartons)": """
SELECT j.nomjoueur, j.position, j.nationalite, e.nomequipe,
       SUM(sj.cartonsjaunes + sj.cartonsrouges) as total_cartons,
       SUM(sj.cartonsjaunes) as jaunes,
       SUM(sj.cartonsrouges) as rouges
FROM joueur j
JOIN statistiquejoueur sj ON j.idjoueur = sj.idjoueur
JOIN equipe e ON j.id_equipe = e.idequipe
{excl}
GROUP BY j.idjoueur
ORDER BY total_cartons ASC
LIMIT 10
""",
    "Répartition des nationalités par équipe": """
SELECT e.nomequipe,
       j.nationalite,
       COUNT(*) as nombre_joueurs
FROM joueur j
JOIN equipe e ON j.id_equipe = e.idequipe
{excl}
GROUP BY e.nomequipe, j.nationalite
ORDER BY e.nomequipe, nombre_joueurs DESC
""",
    "Nombre total de buts par équipe": """
SELECT e.nomequipe,
       SUM(rm.butsmarques) as buts_marques
FROM equipe e
JOIN resultatmatch rm ON e.idequipe = rm.idequipe
{excl}
GROUP BY e.nomequipe
ORDER BY buts_marques DESC
""",
    "Moyenne de buts marqués/encaissés par match": """
SELECT e.nomequipe,
       ROUND(AVG(CAST(rm.butsmarques AS FLOAT)), 2) as moyenne_buts_marques,
       ROUND(AVG(CAST(rm.butsconcedes AS FLOAT)), 2) as moyenne_buts_concedes
FROM equipe e
JOIN resultatmatch rm ON e.idequipe = rm.idequipe
{excl}
GROUP BY e.nomequipe
ORDER BY moyenne_buts_marques DESC
""",
    "Classement des équipes": """
SELECT e.nomequipe,
       COUNT(CASE WHEN rm.resultat = 'Victoire' THEN 1 END) as victoires,
       COUNT(CASE WHEN rm.resultat = 'Nul' THEN 1 END) as nuls,
       COUNT(CASE WHEN rm.resultat = 'Défaite' THEN 1 END) as defaites,
       (COUNT(CASE WHEN rm.resultat = 'Victoire' THEN 1 END) * 3 +
        COUNT(CASE WHEN rm.resultat = 'Nul' THEN 1 END)) as points
FROM equipe e
JOIN resultatmatch rm ON e.idequipe = rm.idequipe
{excl}
GROUP BY e.nomequipe
ORDER BY points DESC
""",
    "Meilleures défenses (moins de buts concédés)": """
SELECT e.nomequipe,
       SUM(rm.butsconcedes) as buts_concedes
FROM equipe e
JOIN resultatmatch rm ON e.idequipe = rm.idequipe
{excl}
GROUP BY e.nomequipe
ORDER BY buts_concedes ASC
""",
    "Meilleurs buteurs par équipe": """
SELECT e.nomequipe, j.nomjoueur, MAX(sj.buts) as max_buts
FROM equipe e
JOIN joueur j ON e.idequipe = j.id_equipe
JOIN statistiquejoueur sj ON j.idjoueur = sj.idjoueur
{excl}
GROUP BY e.nomequipe, j.nomjoueur
ORDER BY e.nomequipe, max_buts DESC
""",
    "Nombre total de matchs joués par équipe": """
SELECT e.nomequipe,
       COUNT(DISTINCT rm.idmatch) as total_matchs
FROM equipe e
JOIN resultatmatch rm ON e.idequipe = rm.idequipe
{excl}
GROUP BY e.nomequipe
ORDER BY total_matchs DESC
"""
}

# Create filter
excl = ""
if selected_team != "All Teams":
    excl = f"WHERE e.nomequipe = '{selected_team}'"

# Build query
base_query = queries[selected_analysis]
query = base_query.replace("{excl}", excl)
df = pd.read_sql(text(query), engine)

# Display
st.header(selected_analysis)
st.dataframe(df)

# Chart if applicable
if selected_analysis in ["Top 10 des meilleurs buteurs"]:
    fig = px.bar(df, x='nomjoueur', y='total_buts', title="Top Scorers")
    st.plotly_chart(fig)
elif selected_analysis == "Répartition des nationalités par équipe" and selected_team != "All Teams":
    fig = px.pie(df, values='nombre_joueurs', names='nationalite', title=f"Nationalities in {selected_team}")
    st.plotly_chart(fig)
elif "buts_marques" in df.columns:
    fig = px.bar(df, x='nomequipe', y='buts_marques', title="Total Goals by Team")
    st.plotly_chart(fig)
elif "moyenne_buts_marques" in df.columns:
    fig = px.bar(df, x='nomequipe', y=['moyenne_buts_marques', 'moyenne_buts_concedes'], barmode='group', title="Average Goals")
    st.plotly_chart(fig)
elif selected_analysis == "Classement des équipes":
    fig = px.bar(df, x='nomequipe', y='points', title="Team Rankings")
    st.plotly_chart(fig)
elif "buts_concedes" in df.columns:
    fig = px.bar(df, x='nomequipe', y='buts_concedes', title="Defenses (Goals Conceded)")
    st.plotly_chart(fig)
elif "total_matchs" in df.columns:
    fig = px.bar(df, x='nomequipe', y='total_matchs', title="Matches Played")
    st.plotly_chart(fig)
elif "max_buts" in df.columns:
    fig = px.bar(df, x='nomequipe', y='max_buts', title="Best Scorers per Team")
    st.plotly_chart(fig)
elif "total_decisif" in df.columns:
    fig = px.bar(df, x='nomjoueur', y='total_decisif', title="Most Decisive Players")
    st.plotly_chart(fig)
elif "total_cartons" in df.columns:
    fig = px.bar(df, x='nomjoueur', y='total_cartons', title=" Most Disciplined Players")
    st.plotly_chart(fig)

# Download
st.sidebar.markdown("### Download")
st.sidebar.download_button("Download CSV", df.to_csv(index=False), file_name='filtered_data.csv', mime='text/csv')
