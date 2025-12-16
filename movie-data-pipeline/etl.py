import pandas as pd
import sqlite3
import requests
import time

# =========================
# CONFIGURATION
# =========================
OMDB_API_KEY = "875c5393"
DB_NAME = "movies.db"

print("ETL script started...")

# =========================
# EXTRACT
# =========================
def extract_data():
    print("Reading CSV files...")
    movies_df = pd.read_csv("movies.csv")
    ratings_df = pd.read_csv("ratings.csv")
    return movies_df, ratings_df


# =========================
# OMDB API CALL
# =========================
def fetch_omdb_data(title):
    url = "http://www.omdbapi.com/"
    params = {
        "t": title,
        "apikey": OMDB_API_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        if data.get("Response") == "True":
            return data
    except Exception as e:
        print("API error:", e)

    return None


# =========================
# TRANSFORM
# =========================
def transform_movies(movies_df):
    print("Transforming and enriching movie data...")
    enriched_movies = []

    for _, row in movies_df.head(10).iterrows():
        movie_id = int(row["movieId"])
        title = row["title"].rsplit("(", 1)[0].strip()
        genres = row["genres"].split("|")
        print(f"Fetching OMDb data for: {title}", flush=True)
        omdb = fetch_omdb_data(title)
        time.sleep(0.2)  # API rate limit

        enriched_movies.append({
            "movie_id": movie_id,
            "title": title,
            "release_year": int(omdb["Year"]) if omdb and omdb.get("Year", "").isdigit() else None,
            "director": omdb.get("Director") if omdb else None,
            "plot": omdb.get("Plot") if omdb else None,
            "box_office": omdb.get("BoxOffice") if omdb else None,
            "genres": genres
        })

    return enriched_movies


# =========================
# LOAD
# =========================
def load_data(enriched_movies, ratings_df):
    print("Loading data into SQLite database...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for movie in enriched_movies:
        cursor.execute("""
            INSERT OR IGNORE INTO movies
            (movie_id, title, release_year, director, plot, box_office)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            movie["movie_id"],
            movie["title"],
            movie["release_year"],
            movie["director"],
            movie["plot"],
            movie["box_office"]
        ))

        for genre in movie["genres"]:
            cursor.execute(
                "INSERT OR IGNORE INTO genres (genre_name) VALUES (?)",
                (genre,)
            )
            cursor.execute(
                "SELECT genre_id FROM genres WHERE genre_name = ?",
                (genre,)
            )
            genre_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                VALUES (?, ?)
            """, (movie["movie_id"], genre_id))

    for _, row in ratings_df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO ratings
            (user_id, movie_id, rating, rating_timestamp)
            VALUES (?, ?, ?, ?)
        """, (
            int(row["userId"]),
            int(row["movieId"]),
            float(row["rating"]),
            int(row["timestamp"])
        ))

    conn.commit()
    conn.close()
    print("Data loaded successfully.")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    movies_df, ratings_df = extract_data()
    enriched_movies = transform_movies(movies_df)
    load_data(enriched_movies, ratings_df)
    print("ETL Pipeline completed successfully ✅")

# OMDB_API_KEY="http://www.omdbapi.com/?i=tt3896198&apikey=875c5393"