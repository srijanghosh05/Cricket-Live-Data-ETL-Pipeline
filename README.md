# Cricket Live Data ETL Pipeline

A local-first, production-quality Python ETL pipeline that pulls live cricket match data from [CricketData.org](https://cricketdata.org/), transforms it, and loads it into an SQLite database using an idempotent upsert strategy.

## Data Architecture

This project follows a medallion-style data flow:

```
Bronze  →  Silver  →  Gold
Raw JSON   Cleaned CSV   SQLite DB
(data/bronze/)  (data/silver/)  (data/cricket.db)
```

- **Bronze Layer**: Immutable raw API responses, timestamped for a full audit trail (Data Lineage).
- **Silver Layer**: Flattened, validated, parsed CSV files — ready for preliminary analysis.
- **Gold Layer**: Upserted SQLite table `matches` — fully queryable, deduplicated, enriched with an `ingested_at` audit timestamp.

## Getting Started

1. Clone or download this repository.
2. Install Python 3.8+.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the ETL pipeline:
   ```bash
   python main.py
   ```
