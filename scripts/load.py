import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def get_db_connection_url():
    """Constructs the SQLAlchemy connection URL from environment variables."""
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    if not all([db_host, db_name, db_user, db_password]):
        logging.error("Missing database environment variables in .env")
        sys.exit(1)
        
    # Format: postgresql+psycopg2://user:password@host:port/dbname
    return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def load_data() -> int:
    """
    Reads the cleaned CSV from the Silver layer and upserts it into the 
    PostgreSQL database in the Gold layer using a Schema-First approach.
    Returns the number of rows loaded.
    """
    project_root = Path(__file__).resolve().parent.parent
    silver_path = project_root / "data" / "silver" / "match_updates.csv"
    
    if not silver_path.exists():
        logging.error(f"Silver data not found at {silver_path}")
        sys.exit(1)
        
    try:
        df = pd.read_csv(silver_path)
    except Exception as e:
        logging.error(f"Error reading silver CSV: {e}")
        sys.exit(1)
        
    if df.empty:
        logging.warning("Silver CSV is empty. Nothing to load.")
        return 0
        
    # Create SQLAlchemy engine
    engine = create_engine(get_db_connection_url())
        
    # Schema-First approach: Create table if it doesn't exist (PostgreSQL syntax)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS matches (
        match_id TEXT PRIMARY KEY,
        match_name TEXT,
        status TEXT,
        venue TEXT,
        runs_team1 INTEGER,
        runs_team2 INTEGER,
        last_updated TEXT,
        ingested_at TIMESTAMP WITH TIME ZONE
    )
    """
    
    # PostgreSQL specific upsert syntax
    upsert_sql = """
    INSERT INTO matches (
        match_id, match_name, status, venue, runs_team1, runs_team2, last_updated, ingested_at
    ) VALUES (
        :match_id, :match_name, :status, :venue, :runs_team1, :runs_team2, :last_updated, :ingested_at
    )
    ON CONFLICT (match_id) DO UPDATE SET
        match_name = EXCLUDED.match_name,
        status = EXCLUDED.status,
        venue = EXCLUDED.venue,
        runs_team1 = EXCLUDED.runs_team1,
        runs_team2 = EXCLUDED.runs_team2,
        last_updated = EXCLUDED.last_updated,
        ingested_at = EXCLUDED.ingested_at;
    """
    
    try:
        with engine.begin() as conn:
            # Create table
            conn.execute(text(create_table_sql))
            
            # Prepare records for insertion
            records_to_insert = []
            now_utc = datetime.now(timezone.utc)
            
            for _, row in df.iterrows():
                # Handle pandas NaNs/NaTs safely
                r1 = int(row['runs_team1']) if pd.notna(row.get('runs_team1')) else None
                r2 = int(row['runs_team2']) if pd.notna(row.get('runs_team2')) else None
                
                # date column mapped to last_updated
                last_updated = str(row['date']) if pd.notna(row.get('date')) else None
                
                records_to_insert.append({
                    "match_id": str(row['match_id']),
                    "match_name": str(row['match_name']) if pd.notna(row.get('match_name')) else None,
                    "status": str(row['status']) if pd.notna(row.get('status')) else None,
                    "venue": str(row['venue']) if pd.notna(row.get('venue')) else None,
                    "runs_team1": r1,
                    "runs_team2": r2,
                    "last_updated": last_updated,
                    "ingested_at": now_utc
                })
            
            # Execute idempotent upsert
            for record in records_to_insert:
                 conn.execute(text(upsert_sql), record)
            
            return len(records_to_insert)
            
    except Exception as e:
        logging.error(f"Error loading data into PostgreSQL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure basic logging for direct execution testing
    logging.basicConfig(level=logging.INFO)
    count = load_data()
    logging.info(f"Loaded {count} matches into DB.")
