-- ============================================================
-- MovieLens ELT Pipeline — Snowflake Setup
-- ============================================================
-- Run this script as ACCOUNTADMIN in Snowflake
-- Replace all <placeholders> with your actual values
-- ============================================================


-- ============================================================
-- PART 1: User, Role & Warehouse Setup
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Create transform role
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create DBT service user
CREATE USER IF NOT EXISTS dbt
  PASSWORD='<your_dbt_password>'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW'
  COMMENT='DBT user used for data transformation';

ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;


-- ============================================================
-- PART 2: Database & Schema Setup
-- ============================================================

CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.DEV;

-- Grant permissions to transform role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;

SHOW SCHEMAS IN DATABASE MOVIELENS;


-- ============================================================
-- PART 3: S3 External Stage
-- ============================================================
-- Prerequisites:
-- 1. Create an S3 bucket and upload MovieLens CSV files
-- 2. Create an AWS IAM user with S3 read access
-- 3. Generate AWS access key and secret for the IAM user

USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVIELENS;
USE SCHEMA RAW;

CREATE STAGE movielens_stage
  URL='s3://<your_s3_bucket_name>'
  CREDENTIALS=(
    AWS_KEY_ID='<your_aws_key_id>'
    AWS_SECRET_KEY='<your_aws_secret_key>'
  );


-- ============================================================
-- PART 4: Raw Table Creation & Data Loading
-- ============================================================

-- Movies
CREATE OR REPLACE TABLE raw_movies (
  movieId INTEGER,
  title   STRING,
  genres  STRING
);
COPY INTO raw_movies
FROM '@movielens_stage/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Ratings
CREATE OR REPLACE TABLE raw_ratings (
  userId    INTEGER,
  movieId   INTEGER,
  rating    FLOAT,
  timestamp BIGINT
);
COPY INTO raw_ratings
FROM '@movielens_stage/ratings.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Tags
CREATE OR REPLACE TABLE raw_tags (
  userId    INTEGER,
  movieId   INTEGER,
  tag       STRING,
  timestamp BIGINT
);
COPY INTO raw_tags
FROM '@movielens_stage/tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Genome Scores
CREATE OR REPLACE TABLE raw_genome_scores (
  movieId   INTEGER,
  tagId     INTEGER,
  relevance FLOAT
);
COPY INTO raw_genome_scores
FROM '@movielens_stage/genome-scores.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Genome Tags
CREATE OR REPLACE TABLE raw_genome_tags (
  tagId INTEGER,
  tag   STRING
);
COPY INTO raw_genome_tags
FROM '@movielens_stage/genome-tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Links
CREATE OR REPLACE TABLE raw_links (
  movieId INTEGER,
  imdbId  INTEGER,
  tmdbId  INTEGER
);
COPY INTO raw_links
FROM '@movielens_stage/links.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- ============================================================
-- PART 5: Validation Queries
-- ============================================================

-- Check latest ratings (after DBT run)
SELECT * FROM MOVIELENS.DEV.FCT_RATINGS
ORDER BY RATING_TIMESTAMP DESC
LIMIT 5;

-- Check staging ratings
SELECT * FROM MOVIELENS.DEV.SRC_RATINGS
ORDER BY RATING_TIMESTAMP DESC
LIMIT 5;


-- ============================================================
-- PART 6: Incremental Load Test
-- ============================================================
-- Insert a new rating to test incremental model

INSERT INTO MOVIELENS.DEV.SRC_RATINGS (user_id, movie_id, rating, rating_timestamp)
VALUES (87587, '7151', '4.0', '2015-03-31 23:40:02.000 -0700');


-- ============================================================
-- PART 7: Snapshot Test
-- ============================================================
-- Check snapshot history for a user

SELECT * FROM SNAPSHOTS.SNAP_TAGS
WHERE user_id = 18
ORDER BY user_id, dbt_valid_from DESC;

-- Update a tag to trigger SCD Type 2 snapshot
UPDATE dev.src_tags
SET tag = 'Mark Waters Returns',
    tag_timestamp = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)
WHERE user_id = 18;

-- Verify the update
SELECT * FROM dev.src_tags
WHERE user_id = 18;


-- ============================================================
-- PART 8: Movie Analysis
-- ============================================================
-- Top 20 highest rated movies with at least 100 ratings

CREATE TABLE movie_analysis AS (
  WITH ratings_summary AS (
    SELECT
      movie_id,
      AVG(rating)   AS average_rating,
      COUNT(*)      AS total_ratings
    FROM MOVIELENS.DEV.FCT_RATINGS
    GROUP BY movie_id
    HAVING COUNT(*) > 100
  )
  SELECT
    m.movie_title,
    rs.average_rating,
    rs.total_ratings
  FROM ratings_summary rs
  JOIN MOVIELENS.DEV.DIM_MOVIES m ON m.movie_id = rs.movie_id
  ORDER BY rs.average_rating DESC
  LIMIT 20
);