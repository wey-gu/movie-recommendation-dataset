{{ config(materialized='table') }}

/*
 JOIN the movieielens_ratings table with the movieielens_movies table, and removing the movie title tailing the year of release
 */
WITH user_watched_movies AS(
    SELECT moveielens_ratings."userId",
        moveielens_ratings."movieId",
        moveielens_ratings.rating,
        REGEXP_REPLACE(moveielens_movies.title, ' \(\d{4}\)$', '') AS title,
        moveielens_movies.genres AS movielens_genres
    FROM moveielens_ratings
        JOIN moveielens_movies ON moveielens_movies."movieId" = moveielens_ratings."movieId"
)
/* 
 JOIN user_watched_movies table with all_movie_aliase_iso table where language is English
 the join condition is the movie title
 */
SELECT concat('u_',user_watched_movies."userId") AS user_id,
    user_watched_movies.rating,
    user_watched_movies.title,
    all_movie_aliases_iso."movie_id" AS OMDB_movie_id,
    user_watched_movies.movielens_genres
FROM user_watched_movies
    JOIN all_movie_aliases_iso ON user_watched_movies.title LIKE CONCAT(all_movie_aliases_iso.name, '%')
    AND all_movie_aliases_iso.language_iso_639_1 = 'en'