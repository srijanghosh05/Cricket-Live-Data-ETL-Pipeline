# Cricket Live Data ETL Pipeline

A production-quality Python ETL pipeline that pulls live cricket match data from [CricketData.org](https://cricketdata.org/), transforms it, and loads it into an AWS RDS (PostgreSQL) database using an idempotent upsert strategy.

## Architecture

This project follows a medallion-style data flow, designed to run on a scheduled AWS EC2 Ubuntu instance:

```text
Bronze (Raw)         →  Silver (Transformed)   →  Gold (Served)
Raw JSON files          Cleaned CSV               AWS RDS PostgreSQL
(data/bronze/)          (data/silver/)            (cricket-db)
```

- **Bronze Layer**: Immutable raw API responses, timestamped for a full audit trail (Data Lineage).
- **Silver Layer**: Flattened, validated, parsed CSV files — ready for preliminary analysis.
- **Gold Layer**: Upserted PostgreSQL table `matches` — fully queryable, deduplicated, enriched with an `ingested_at` audit timestamp.

## Getting Started

### 1. Prerequisites
- Python 3.8+
- An AWS RDS PostgreSQL instance
- A CricketData.org API Key

### 2. Installation
Clone the repository and install dependencies:
```bash
git clone https://github.com/srijanghosh05/Cricket-Live-Data-ETL-Pipeline.git
cd Cricket-Live-Data-ETL-Pipeline
pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the root directory and add your credentials:
```env
CRICKETDATA_API_KEY=your_api_key_here
DB_HOST=your-rds-endpoint.aws.com
DB_PORT=5432
DB_NAME=cricket
DB_USER=postgres
DB_PASSWORD=your_db_password
```

### 4. Running the Pipeline
Run the orchestrator script to execute Extract, Transform, and Load sequentially:
```bash
python main.py
```

### AWS EC2 Deployment (Cron)
This pipeline uses absolute path resolution `Path(__file__).resolve().parent.parent` to ensure it runs flawlessly via Linux `cron` on an EC2 instance.
Example cron job (runs every 30 minutes):
```bash
*/30 * * * * cd /home/ubuntu/Cricket-Live-Data-ETL-Pipeline && /home/ubuntu/Cricket-Live-Data-ETL-Pipeline/venv/bin/python main.py >> pipeline.log 2>&1
```
