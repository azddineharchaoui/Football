import sqlite3

conn = sqlite3.connect('football_db.db')
c = conn.cursor()

c.execute('SELECT COUNT(*) FROM equipe')
print("Equipes:", c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM joueur')
print("Joueurs:", c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM resultatmatch')
print("Resultat match:", c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM match')
print("Match:", c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM statistiquejoueur')
print("Stat joueurs:", c.fetchone()[0])

conn.close()
