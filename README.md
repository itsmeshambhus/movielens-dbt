# 🎬 MovieLens ELT Pipeline

 
An end-to-end data engineering project built on the [MovieLens 20M dataset](https://grouplens.org/datasets/movielens/20m/) — covering the full ELT pipeline from raw CSV files on Amazon S3 to analytics-ready tables in Snowflake, transformed using **DBT (Data Build Tool)**.
 
---
 
## 🏗️ Architecture
 
```
MovieLens CSVs (6 files)
        ↓
Amazon S3 Bucket (raw storage)
        ↓
AWS IAM User (secure Snowflake access)
        ↓
Snowflake External Stage (S3 → Snowflake)
        ↓
COPY INTO → Raw Tables (6 tables)
        ↓
DBT Staging Models (clean & rename)
        ↓
DBT Dim + Fact Models (business logic)
        ↓
DBT Mart Models (analytics-ready tables)
```
 
---
 
## 🛠️ Tech Stack
 
| Layer | Tool | Purpose |
|---|---|---|
| Storage | Amazon S3 | Store raw MovieLens CSV files |
| Security | AWS IAM | Dedicated user with S3 read policy for Snowflake |
| Warehouse | Snowflake | Cloud data warehouse — external stage + raw tables |
| Transform | DBT Core | Modular SQL transformation pipeline |
| Packages | dbt_utils | Surrogate key generation and utilities |
| Language | SQL + Jinja | Model and macro authoring |
 
---
 
## ☁️ Infrastructure Setup
 
### Step 1 — Amazon S3
- Created an S3 bucket and uploaded all 6 MovieLens CSV files:
  - `movies.csv`, `ratings.csv`, `tags.csv`
  - `genome-scores.csv`, `genome-tags.csv`, `links.csv`
### Step 2 — AWS IAM User
- Created a dedicated IAM user for Snowflake access
- Attached an S3 read policy — following the principle of least privilege
- Generated AWS access key and secret for use in Snowflake stage
### Step 3 — Snowflake External Stage
```sql
CREATE STAGE movielens_stage
  URL='s3://your-bucket-name'
  CREDENTIALS=(
    AWS_KEY_ID='<your_aws_key_id>'
    AWS_SECRET_KEY='<your_aws_secret_key>'
  );
```
 
### Step 4 — Load Raw Tables with COPY INTO
```sql
CREATE OR REPLACE TABLE raw_movies (
  movieId INTEGER,
  title STRING,
  genres STRING
);
 
COPY INTO raw_movies
FROM '@movielens_stage/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```
> See full setup SQL in [`setup/snowflake_setup.sql`](setup/snowflake_setup.sql)
 
---
 
## 📁 DBT Project Structure
 
```
movielens/
├── models/
│   ├── staging/         → Source-aligned staging models (src_*)
│   ├── dim/             → Dimension tables (dim_*)
│   ├── fct/             → Fact tables (fct_*)
│   └── mart/            → Final mart tables for reporting
├── snapshots/           → SCD Type 2 tracking (snap_tags)
├── seeds/               → Static CSV reference data
├── macros/              → Reusable Jinja SQL macros
├── tests/               → Custom data quality tests
├── analyses/            → Ad-hoc SQL exploration queries
├── packages.yml         → dbt_utils package dependency
└── dbt_project.yml      → Project configuration
```
 
---
 
## 🧩 DBT Models Overview
 
### Staging Layer
| Model | Description |
|---|---|
| `src_movies` | Raw movies with renamed columns |
| `src_ratings` | Raw user ratings with timestamp conversion |
| `src_tags` | Raw user tags with timestamp conversion |
| `src_links` | Movie external links (IMDB, TMDB) |
| `src_genome_tags` | Genome tag labels |
| `src_genome_score` | Genome relevance scores per movie/tag |
 
### Dimension Layer
| Model | Description |
|---|---|
| `dim_movies` | Cleaned movie metadata with genre array |
| `dim_users` | Unique users derived from ratings + tags |
| `dim_genome_tags` | Standardized genome tag names |
| `dim_movies_with_tags` | Ephemeral model joining movies, tags & scores |
 
### Fact Layer
| Model | Description |
|---|---|
| `fct_ratings` | Incremental fact table of user ratings |
| `fct_genome_scores` | Relevance scores per movie and tag |
 
### Mart Layer
| Model | Description |
|---|---|
| `mart_movie_releases` | Ratings joined with seed release dates |
 
---
 
## ⚡ Key DBT Features Used
 
- **Incremental model** — `fct_ratings` loads only new records using `is_incremental()`
- **Ephemeral model** — `dim_movies_with_tags` used as a reusable CTE, not materialized
- **Snapshot** — `snap_tags` tracks historical tag changes (SCD Type 2) using timestamp strategy + `dbt_utils.generate_surrogate_key`
- **Seeds** — `seed_movie_release_dates.csv` for static reference data
- **Custom Macro** — `no_nulls_in_columns` iterates all columns using Jinja loop
- **Custom Test** — `relevence_score_test.sql` using the macro on `fct_genome_scores`
- **Schema Tests** — `not_null` and `relationships` tests defined in `schema.yml`
- **Packages** — `dbt_utils` for surrogate key generation
- **Analysis** — `movie_analysis.sql` for top-rated movies exploration
---
 
## 🧪 Testing
 
```bash
dbt test                          # Run all tests
dbt test --select fct_ratings     # Run specific model test
```
 
Tests include:
- `not_null` on all primary and foreign keys
- `relationships` between `fct_ratings.movie_id` → `dim_movies.movie_id`
- Custom null check macro on `fct_genome_scores`
---
 
## 🚀 Getting Started
 
### Prerequisites
- Python 3.8+
- dbt-snowflake installed
- Snowflake account
- AWS account with S3 bucket and IAM user
### Setup
 
```bash
# Clone the repo
git clone https://github.com/itsmeshambhus/movielens-dbt.git
cd movielens-dbt
 
# Install dbt packages
dbt deps
 
# Configure your Snowflake connection
# Create ~/.dbt/profiles.yml with your credentials
 
# Run the full pipeline
dbt build
```
 
### Common Commands
 
```bash
dbt run                              # Run all models
dbt test                             # Run all tests
dbt build                            # Run models + tests
dbt seed                             # Load seed files
dbt snapshot                         # Run snapshots
dbt docs generate && dbt docs serve  # View documentation
```
 
---
 
## 📊 Dataset
 
**MovieLens 20M** — a well-known dataset from GroupLens Research containing:
- ~27,000 movies
- ~20 million ratings from ~138,000 users
- Genome tags and relevance scores
- User-generated tags with timestamps
---
 
## 📚 What I Learned
 
This project covers the **full modern data engineering stack**:
- Setting up cloud storage on **AWS S3**
- Managing secure access with **AWS IAM**
- Loading data into **Snowflake** using external stages and COPY INTO
- Building a complete **DBT transformation pipeline** with modular models, testing, documentation, snapshots, macros, and incremental loading
---
 
## 📬 Connect

Built by **Shambhu Prasad Sah** — [LinkedIn](https://www.linkedin.com/in/sahshambhu/) · [GitHub](https://github.com/itsmeshambhus)