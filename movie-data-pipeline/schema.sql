CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT,
    release_year INTEGER,
    director TEXT,
    plot TEXT,
    box_office TEXT
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id INTEGER,
    movie_id INTEGER,
    rating REAL,
    rating_timestamp INTEGER,
    PRIMARY KEY (user_id, movie_id)
);
