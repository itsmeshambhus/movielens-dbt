# 🎬 MovieLens DBT Project

A end-to-end data transformation project built with **DBT (Data Build Tool)** using the [MovieLens dataset](https://grouplens.org/datasets/movielens/).

This project demonstrates modern analytics engineering practices — modular SQL models, data testing, snapshots, macros, seeds, and incremental loading — all running on **Snowflake**.

---

## 📐 Architecture

```
Raw Data (Snowflake)
      ↓
Staging Models       → clean & rename raw tables
      ↓
Dimension Models     → enriched, analytics-ready dimensions
      ↓
Fact Models          → measurable business events
      ↓
Mart Models          → final tables for BI & reporting
```

---

## 📁 Project Structure

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

## 🧩 Models Overview

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
# Run all tests
dbt test

# Run specific model test
dbt test --select fct_ratings
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
- Snowflake account with MovieLens raw data loaded

### Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/movielens-dbt.git
cd movielens-dbt

# Install dbt packages
dbt deps

# Configure your Snowflake connection
# Create ~/.dbt/profiles.yml with your credentials

# Run the project
dbt build
```

### Common Commands

```bash
dbt run          # Run all models
dbt test         # Run all tests
dbt build        # Run models + tests together
dbt seed         # Load seed files
dbt snapshot     # Run snapshots
dbt docs generate && dbt docs serve  # View documentation
```

---

## 📊 Dataset

**MovieLens** — a well-known dataset from GroupLens Research containing:
- ~9,000 movies
- ~100,000 ratings from ~600 users
- Genome tags and relevance scores
- User-generated tags with timestamps

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| DBT Core | Data transformation framework |
| Snowflake | Cloud data warehouse |
| dbt_utils | Community utility macros |
| SQL + Jinja | Model and macro authoring |

---

## 📚 What I Learned

This project was built as part of my **Data Engineering learning journey** to practice real-world analytics engineering using the full DBT feature set.

---

## 📬 Connect

Built by **[Shambhu Prasad Sah]** — [LinkedIn](https://www.linkedin.com/in/sahshambhu/) · [GitHub](https://github.com/itsmeshambhus)