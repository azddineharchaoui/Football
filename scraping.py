import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from io import StringIO
import time

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
driver = webdriver.Chrome(options=options)

main_url = "https://fbref.com/en/comps/9/2024-2025/2024-2025-Premier-League-Stats"
driver.get(main_url)
time.sleep(3)

# Extract team links as WebElements for clicking
all_links = driver.find_elements("tag name", "a")
team_links = []

for link in all_links:
    href = link.get_attribute("href")
    if href and "/squads/" in href and "2024-2025" in href:
        text = link.text.strip()
        if text and not any(existing.text.strip() == text for existing in team_links):
            team_links.append(link)

print(f"Found {len(team_links)} unique team links:")
team_names = []
for link in team_links:
    name = link.text.strip()
    team_names.append(name)
    print(f"- {name}")

all_data = {}

for i, team_link in enumerate(team_links):
    team_name = team_link.text.strip()
    print(f"\nScraping {team_name}... ({i+1}/{len(team_links)})")
    team_link.click()
    time.sleep(3)  # Wait for page to load after click

    players_df = pd.DataFrame()
    try:
        players_tables = pd.read_html(StringIO(driver.page_source), header=[0, 1])
        for table in players_tables:
            if 'Player' in str(table.columns) and len(table) > 0:
                players_df = table.copy()

                players_df.columns = ['_'.join(str(col).strip() for col in column if col).strip('_')
                                     for column in players_df.columns]

                player_col = None
                for col in players_df.columns:
                    if 'player' in col.lower() and 'player' == col.lower().split('_')[0]:
                        player_col = col
                        break
                    

                if player_col is None:
                    continue

                players_df = players_df[
                    players_df[player_col].notna() &
                    ~players_df[player_col].str.contains('Total|Squad Total', case=False, na=False)
                ]

                if len(players_df) > 0:
                    relevant_cols = [col for col in players_df.columns
                                   if any(k in col.lower() for k in
                                         ['player', 'nation', 'pos', 'age', 'mp',
                                          'starts', 'min', 'gls', 'ast', 'xg', 'crd'])]
                    players_df = players_df[relevant_cols].dropna(axis=1, how='all')
                    print(f"Extracted {len(players_df)} players")
                    break

        if players_df.empty:
            print("No player stats found")
    except Exception as e:
        print(f"Error scraping players: {e}")

    matches_df = pd.DataFrame()
    try:
        matches_tables = pd.read_html(StringIO(driver.page_source))
        for table in matches_tables:
            if 'Date' in str(table.columns) and 'Comp' in str(table.columns) and len(table) > 0:
                matches_df = table
                break
        if not matches_df.empty:
            matches_df = matches_df.dropna(subset=['Date'])
            relevant_cols = [col for col in ['Date', 'Time', 'Comp', 'Round', 'Venue',
                                            'Result', 'GF', 'GA', 'Opponent', 'xG', 'xGA', 'Poss']
                           if col in matches_df.columns]
            matches_df = matches_df[relevant_cols].dropna(axis=1, how='all')
            if 'Comp' in matches_df.columns:
                matches_df = matches_df[matches_df['Comp'].str.contains('Premier League', na=False)]
            print(f"Extracted {len(matches_df)} Premier League matches")
        else:
            print("No matches found")
    except Exception as e:
        print(f"Error scraping matches: {e}")

    all_data[team_name] = {'players': players_df, 'matches': matches_df}

    # Navigate back to the main page using browser history
    driver.back()
    time.sleep(3)

players_dfs = [all_data[team]['players'].assign(Team=team)
               for team in all_data if not all_data[team]['players'].empty]
all_players = pd.concat(players_dfs, ignore_index=True) if players_dfs else pd.DataFrame()

matches_dfs = [all_data[team]['matches'].assign(Team=team)
               for team in all_data if not all_data[team]['matches'].empty]
all_matches = pd.concat(matches_dfs, ignore_index=True) if matches_dfs else pd.DataFrame()

if not all_players.empty:
    all_players.to_csv('premier_league_players_2024_2025.csv', index=False)
    print(f"\n✓ Players saved: {all_players.shape[0]} rows × {all_players.shape[1]} columns")
else:
    print("\n✗ No player data found.")

if not all_matches.empty:
    all_matches.to_csv('premier_league_matches_2024_2025.csv', index=False)
    print(f"✓ Matches saved: {all_matches.shape[0]} rows × {all_matches.shape[1]} columns")
else:
    print("✗ No match data found.")

driver.quit()
