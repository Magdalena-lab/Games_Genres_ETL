CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.games (
    game_id SERIAL PRIMARY KEY,
    rawg_game_id INT UNIQUE NOT NULL, 
    game_name TEXT NOT NULL,
    release_date DATE,
    age_rating TEXT,
    ratings_count INT,
    rating REAL,
    created_at TIMESTAMP DEFAULT NOW() 
);

CREATE TABLE IF NOT EXISTS raw.genres (
    genre_id SERIAL PRIMARY KEY,
    rawg_genre_id INT UNIQUE NOT NULL,
    genre_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.bridge_games_genres (
    game_id INT NOT NULL REFERENCES raw.games(game_id),
    genre_id INT NOT NULL REFERENCES raw.genres(genre_id),
    PRIMARY KEY (game_id, genre_id)
);

CREATE TABLE IF NOT EXISTS raw.platforms (
    platform_id SERIAL PRIMARY KEY,
    rawg_platform_id INT UNIQUE NOT NULL,
    platform_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.bridge_platforms_games (
    game_id INT NOT NULL REFERENCES raw.games(game_id),
    platform_id INT NOT NULL REFERENCES raw.platforms(platform_id),
    PRIMARY KEY (game_id, platform_id)
);


