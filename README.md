# ETL_GAMES

A Python ETL pipeline that extracts video game data from the [RAWG API](https://rawg.io/apidocs), 
loads it into PostgreSQL, and generates JSON files for offline analysis. This project demonstrates working with APIs, 
handling nested data, building many-to-many bridges, and integrating with a relational database.

Features:

- Pulls games released from 2022–2024 across multiple platforms (PC, PlayStation, Xbox, iOS, Android, Nintendo, Web).  
- Stores data in PostgreSQL tables:
  - `games`
  - `genres`
  - `platforms`
  - `bridge_games_genres`
  - `bridge_platforms_games`
- Handles many-to-many relationships via bridge tables.  
- Saves JSON snapshots for offline use:
  - `games_raw.json`
  - `genres_raw.json`
  - `platforms_raw.json`
  - `bridge_games_genres.json`
  - `bridge_platforms_games.json`
- Implements API rate-limiting:
  - 2–3 second pause between calls.
  - 10-minute pause after every 50 calls.
- Graceful error handling; inserts use `ON CONFLICT DO NOTHING` to avoid duplicates.

Setup:

1. Clone the repository:

```bash
git clone https://github.com/Magdalena-lab/Games_Genres_ETL.git
cd ETL_GAMES/rawg_games
