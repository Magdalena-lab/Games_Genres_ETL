import os
import psycopg2
import requests
import pandas as pd
from dotenv import load_dotenv
import time
from sqlalchemy import create_engine
import json


load_dotenv()
api_key = os.getenv("game_api_key")
db_name = os.getenv ("pg_db")
db_host = os.getenv("pg_host")
db_user = os.getenv("pg_user")
db_password = os.getenv("pg_password")
db_port = os.getenv("pg_port")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

if not all([api_key, db_host, db_name, db_user, db_password, db_port]):
    raise ValueError("Missing DB credentials or API key in .env file")

# ___________Connection PostgreSQL___________

conn = psycopg2.connect(
    host=db_host,
    dbname=db_name,
    user=db_user,
    password=db_password,
    port=db_port
)
cursor = conn.cursor()

#____________RAWG API___________________

url = "https://api.rawg.io/api/games"
start_date = "2022-01-01"
end_date = "2024-12-31"

parent_platforms = {
    "PC": 1,
    "PlayStation": 2,
    "Xbox": 3,
    "iOS": 4,
    "Android": 8,
    "Nintendo": 7,
    "Web": 14
}

target_per_platform = 400
page_size = 40
sleep_per_call = 3
batch_size = 50
sleep_between_batches = 10 * 60


games_list=[]
genres_list=[]
platforms_list=[]
games_genres=[]
games_platforms=[]


for platform_name, parent_id in parent_platforms.items():
    print(f"\nStarting platform: {platform_name}")
    collected_games = 0
    page = 1
    api_calls = 0
    
    while collected_games < target_per_platform:
        params = {
            "key": api_key,
            "dates": f"{start_date},{end_date}",
            "page_size": page_size,
            "page": page,
            "parent_platforms": parent_id,
            "ordering": "ratings_count"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                print(f"No more data for {platform_name}. Stopping.")
                break
            print(f"API call #{api_calls + 1} for {platform_name} succeeded")
        except requests.exceptions.RequestException as e:
            print(f"API call #{api_calls + 1} for {platform_name} failed:", e)
            break
        
        page += 1
        api_calls += 1
        collected_games += len(results)
        
            
    print(f"API call #{api_calls}")
    
    time.sleep(sleep_per_call)
    
    if api_calls % batch_size == 0:
            print(f"Reached {api_calls} calls for {platform_name}, sleeping for {sleep_between_batches/60} minutes")
            time.sleep(sleep_between_batches)

    #_____________games_______________
    
    data = response.json()['results']
    
    df_games = pd.json_normalize(data)
    games_cols = ['id','name','released','esrb_rating.name','ratings_count','rating']
    
    existing_cols = [c for c in games_cols if c in df_games.columns]
    df_games = df_games[existing_cols]

    for c in games_cols:
        if c not in df_games.columns:
            df_games[c] = None
    
    df_games.rename(columns={
                    'id':'rawg_game_id',
                    'name':'game_name',
                    'released':'release_date',
                    'esrb_rating.name':'age_rating',
                    'ratings_count':'ratings_count',
                    'rating':'rating'},
                    inplace=True) 
    
    
    games_list.extend(df_games.to_dict(orient="records"))

    
    for index, row in df_games.iterrows():
        try:
            cursor.execute("""
            INSERT INTO raw.games (rawg_game_id, game_name, release_date, age_rating, ratings_count, rating)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (rawg_game_id) DO NOTHING
            """, (row['rawg_game_id'], row['game_name'], row['release_date'], row['age_rating'],row['ratings_count'],row['rating']))
        except Exception as e:
            print(f"Insert failed at row {index}: {e}")
            
    #_____________genres____________
    
    df_genres = pd.json_normalize(results, record_path=['genres'])[['id','name']]
    df_genres.rename(columns={
                    'id':'rawg_genre_id',
                    'name':'genre_name',
                    },
                    inplace=True)
    df_genres = df_genres.drop_duplicates().reset_index(drop=True)
    
    genres_list.extend(df_genres.to_dict(orient="records"))
    
    for index, row in df_genres.iterrows():
        try:
            cursor.execute("""
            INSERT INTO raw.genres (rawg_genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (rawg_genre_id) DO NOTHING
            """, (row['rawg_genre_id'], row['genre_name']))
        except Exception as e:
            print(f"Insert failed at row {index}: {e}")
    
    #______________platforms___________________
    
    df_platforms = pd.json_normalize(results, record_path=['platforms'])[['platform.id','platform.name']]
    df_platforms.rename(columns={
                    'platform.id':'rawg_platform_id',
                    'platform.name':'platform_name',},
                    inplace=True)
    df_platforms = df_platforms.drop_duplicates().reset_index(drop=True) 
    
    platforms_list.extend(df_platforms.to_dict(orient="records"))

    
    for index, row in df_platforms.iterrows():
        try:
            cursor.execute("""
            INSERT INTO raw.platforms (rawg_platform_id, platform_name)
            VALUES (%s, %s)
            ON CONFLICT (rawg_platform_id) DO NOTHING
            """, (row['rawg_platform_id'], row['platform_name']))
        except Exception as e:
            print(f"Insert failed at row {index}: {e}")
    
    conn.commit()
    
    #____________bridge_games_genres________________
    
    df_bridge_games_genres = pd.json_normalize(
    results,
    record_path=['genres'],   
    meta=['id'],             
    meta_prefix='game_'       
    )            


    df_bridge_games_genres.rename(columns={
        'id': 'rawg_genre_id',   
        'game_id': 'rawg_game_id'     
        }, inplace=True)
    

    game_map = pd.read_sql("SELECT game_id, rawg_game_id FROM raw.games", engine)
    genre_map = pd.read_sql("SELECT genre_id, rawg_genre_id FROM raw.genres", engine)

    df_G_bridge = df_bridge_games_genres.merge(game_map, on='rawg_game_id')\
                .merge(genre_map, on='rawg_genre_id')
    
    df_bridge_games_genres = df_G_bridge[['game_id','genre_id']].copy()
    
    games_genres.extend(df_bridge_games_genres.to_dict(orient="records"))


    for index, row in df_bridge_games_genres.iterrows():
        try:
            cursor.execute("""
            INSERT INTO raw.bridge_games_genres (game_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """,(int(row['game_id']), int(row['genre_id']))) 
        except Exception as e:
            print(f"Insert failed at row {index}: {e}")

    #________________bridge_platforms_games____________________
    
    df_bridge_platforms_games = pd.json_normalize(
    results,
    record_path=['platforms'],   
    meta=['id'],             
    meta_prefix='game_'       
    )            


    df_bridge_platforms_games.rename(columns={
        'platform.id': 'rawg_platform_id',   
        'game_id': 'rawg_game_id'     
        }, inplace=True)
    

    game_map = pd.read_sql("SELECT game_id, rawg_game_id FROM raw.games", engine)
    platform_map = pd.read_sql("SELECT platform_id, rawg_platform_id FROM raw.platforms", engine)

    df_P_bridge = df_bridge_platforms_games.merge(game_map, on='rawg_game_id')\
                .merge(platform_map, on='rawg_platform_id')
    
    df_bridge_platforms_games = df_P_bridge[['game_id','platform_id']].copy()
    
    games_platforms.extend(df_bridge_platforms_games.to_dict(orient="records"))
    
    for index, row in df_bridge_platforms_games.iterrows():
        try:
            cursor.execute("""
            INSERT INTO raw.bridge_platforms_games (game_id, platform_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """,(int(row['game_id']), int(row['platform_id']))) 
        except Exception as e:
            print(f"Insert failed at row {index}: {e}")

#__________________save to json______________________

with open("games_raw.json", "w") as f:
    json.dump(games_list, f, indent=4)

with open("genres_raw.json", "w") as f:
    json.dump(genres_list, f, indent=4)

with open("platforms_raw.json", "w") as f:
    json.dump(platforms_list, f, indent=4)

with open("bridge_games_genres.json", "w") as f:
    json.dump(games_genres, f, indent=4)
    
with open("bridge_platforms_games.json", "w") as f:
    json.dump(games_platforms, f, indent=4)


conn.commit()
cursor.close()
conn.close()