CH_TABLES = """
CREATE TABLE IF NOT EXISTS bookmarks (
    user_id Int64,
    movie_id UUID,
    created_at DateTime
) Engine = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, movie_id, created_at);

CREATE TABLE IF NOT EXISTS movies (
    movie_id UUID,
    title String,
    like_counter Int64,
    rating Int8,
    create Int16,
    created_at DateTime
) Engine = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (movie_id, rating, created_at);
"""
