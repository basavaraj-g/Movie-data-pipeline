-- Highest average rating movie
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
ORDER BY avg_rating DESC
LIMIT 1;

-- Top 5 genres
SELECT g.genre_name, AVG(r.rating) AS avg_rating
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC
LIMIT 5;

-- Director with most movies
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- Avg rating per year
SELECT m.release_year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.release_year
ORDER BY m.release_year;
