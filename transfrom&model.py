import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, Time, Enum, ForeignKey, MetaData, Table

engine = create_engine('sqlite:///football_db.db')
metadata = MetaData()

competition = Table('competition', metadata,
    Column('idcompetition', Integer, primary_key=True),
    Column('nomcompetition', String)
)

saison = Table('saison', metadata,
    Column('id_saison', Integer, primary_key=True),
    Column('annee', String)
)

equipe = Table('equipe', metadata,
    Column('idequipe', Integer, primary_key=True),
    Column('nomequipe', String),
    Column('idcompetition', Integer, ForeignKey('competition.idcompetition')),
    Column('idsaison', Integer, ForeignKey('saison.id_saison'))
)

joueur = Table('joueur', metadata,
    Column('idjoueur', Integer, primary_key=True),
    Column('nomjoueur', String),
    Column('position', String),
    Column('nationalite', String),
    Column('id_equipe', Integer, ForeignKey('equipe.idequipe'))
)

match = Table('match', metadata,
    Column('idmatch_', Integer, primary_key=True),
    Column('date_match', Date),
    Column('heure', String),
    Column('round', String),
    Column('venue', String),
    Column('idteamhome', Integer, ForeignKey('equipe.idequipe')),
    Column('idteam__away', Integer, ForeignKey('equipe.idequipe')),
    Column('id_competition', Integer, ForeignKey('competition.idcompetition')),
    Column('id_saison', Integer, ForeignKey('saison.id_saison'))
)

resultatmatch = Table('resultatmatch', metadata,
    Column('idresultat', Integer, primary_key=True),
    Column('idmatch', Integer, ForeignKey('match.idmatch_')),
    Column('idequipe', Integer, ForeignKey('equipe.idequipe')),
    Column('butsmarques', Integer),
    Column('butsconcedes', Integer),
    Column('resultat', Enum('Victoire', 'Défaite', 'Nul', name='resultat_enum'))
)

statistiquejoueur = Table('statistiquejoueur', metadata,
    Column('idstats', Integer, primary_key=True),
    Column('idjoueur', Integer, ForeignKey('joueur.idjoueur')),
    Column('buts', Integer),
    Column('passesdecisives', Integer),
    Column('nbmatchesplayed', Integer),
    Column('cartonsjaunes', Integer),
    Column('cartonsrouges', Integer)
)

metadata.create_all(engine)

players = pd.read_csv('premier_league_players_2024_2025.csv')
matches = pd.read_csv('premier_league_matches_2024_2025.csv')

# Strip column names to remove leading/trailing spaces
players.columns = players.columns.str.strip()
matches.columns = matches.columns.str.strip()

print("-----------------------------------------------------")
print(matches.isnull().sum())
print(players.isnull().sum())

players = players[pd.to_numeric(players['Age'], errors='coerce').notna()]

players.rename(columns={
    'Player': 'nomjoueur',
    'Nation': 'nationalite',
    'Pos': 'position',
    'Performance_CrdY': 'cartonsjaunes',
    'Performance_CrdR': 'cartonsrouges'
}, inplace=True)

players['buts'] = 0
players['passesdecisives'] = 0
players['nbmatchesplayed'] = 1

numeric_cols_players = ['buts', 'passesdecisives', 'nbmatchesplayed', 'cartonsjaunes', 'cartonsrouges']
players[numeric_cols_players] = players[numeric_cols_players].apply(lambda col: pd.to_numeric(col, errors='coerce').fillna(0).astype(int))

players = players[['nomjoueur', 'nationalite', 'position', 'buts', 'passesdecisives', 'nbmatchesplayed', 'cartonsjaunes', 'cartonsrouges', 'Team']].dropna(subset=['nomjoueur', 'Team'])

matches['Date'] = pd.to_datetime(matches['Date'])
numeric_cols_matches = ['GF', 'GA']
matches[numeric_cols_matches] = matches[numeric_cols_matches].fillna(0).astype(int)
matches = matches.dropna(subset=['Date', 'Result', 'Team', 'Opponent'])

conn = engine.connect()

ins_comp = competition.insert().values(nomcompetition='Premier League')
res_comp = conn.execute(ins_comp)
id_comp = res_comp.inserted_primary_key[0]

ins_sai = saison.insert().values(annee='2024-2025')
res_sai = conn.execute(ins_sai)
id_sai = res_sai.inserted_primary_key[0]

unique_teams = pd.unique(pd.concat([matches['Team'], matches['Opponent']]))
team_to_id = {}
for t in unique_teams:
    ins_eq = equipe.insert().values(nomequipe=t, idcompetition=id_comp, idsaison=id_sai)
    res_eq = conn.execute(ins_eq)
    team_to_id[t] = res_eq.inserted_primary_key[0]

for _, row in players.iterrows():
    ins_j = joueur.insert().values(
        nomjoueur=row['nomjoueur'],
        position=row['position'],
        nationalite=row['nationalite'],
        id_equipe=team_to_id[row['Team']]
    )
    res_j = conn.execute(ins_j)
    id_j = res_j.inserted_primary_key[0]
    ins_stat = statistiquejoueur.insert().values(
        idjoueur=id_j,
        buts=row['buts'],
        passesdecisives=row['passesdecisives'],
        nbmatchesplayed=row['nbmatchesplayed'],
        cartonsjaunes=row['cartonsjaunes'],
        cartonsrouges=row['cartonsrouges']
    )
    conn.execute(ins_stat)

home_matches = matches[matches['Venue'] == 'Home']
for _, row in home_matches.iterrows():
    home_id = team_to_id[row['Team']]
    away_id = team_to_id[row['Opponent']]
    ins_m = match.insert().values(
        date_match=row['Date'],
        heure=row['Time'],
        round=row['Round'],
        venue=row['Venue'],
        idteamhome=home_id,
        idteam__away=away_id,
        id_competition=id_comp,
        id_saison=id_sai
    )
    res_m = conn.execute(ins_m)
    id_m = res_m.inserted_primary_key[0]
    resultat_map = {'W': 'Victoire', 'L': 'Défaite', 'D': 'Nul'}
    resultat_home = resultat_map[row['Result']]
    ins_res_h = resultatmatch.insert().values(
        idmatch=id_m,
        idequipe=home_id,
        butsmarques=row['GF'],
        butsconcedes=row['GA'],
        resultat=resultat_home
    )
    conn.execute(ins_res_h)
    resultat_away = {'W': 'Défaite', 'L': 'Victoire', 'D': 'Nul'}[row['Result']]
    ins_res_a = resultatmatch.insert().values(
        idmatch=id_m,
        idequipe=away_id,
        butsmarques=row['GA'],
        butsconcedes=row['GF'],
        resultat=resultat_away
    )
    conn.execute(ins_res_a)

conn.commit()
conn.close()

print("""
@startuml
class Competition {
  idcompetition : int <<PK>>
  nomcompetition : string
}
class Saison {
  id_saison : int <<PK>>
  annee : string
}
class Equipe {
  idequipe : int <<PK>>
  nomequipe : string
}
class Joueur {
  idjoueur : int <<PK>>
  nomjoueur : string
  position : string
  nationalite : string
}
class Match {
  idmatch_ : int <<PK>>
  date_match : date
  heure : time
  round : string
  venue : string
}
class ResultatMatch {
  idresultat : int <<PK>>
  butsmarques : int
  butsconcedes : int
  resultat : enum(Victoire, Défaite, Nul)
}
class StatistiqueJoueur {
  idstats : int <<PK>>
  buts : int
  passesdecisives : int
  nbmatchesplayed : int
  cartonsjaunes : int
  cartonsrouges : int
}
Equipe --> Competition
Equipe --> Saison
Joueur --> Equipe
Match --> Equipe : home
Match --> Equipe : away
Match --> Competition
Match --> Saison
ResultatMatch --> Match
ResultatMatch --> Equipe
StatistiqueJoueur --> Joueur
@enduml
""")
