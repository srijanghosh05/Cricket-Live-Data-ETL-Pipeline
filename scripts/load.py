import sys
import sqlite3
import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

def load_data() -> int:
    """
    Reads the cleaned CSV from the Silver layer and upserts it into the 
    SQLite database in the Gold layer using a Schema-First approach.
    Returns the number of rows loaded.
    """
    db_path = Path("data/cricket.db")
    silver_path = Path("data/silver/match_updates.csv")
    
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
        
    # Schema-First approach: Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS matches (
        match_id TEXT PRIMARY KEY,
        match_name TEXT,
        status TEXT,
        venue TEXT,
        runs_team1 INTEGER,
        runs_team2 INTEGER,
        last_updated TEXT,
        ingested_at TEXT
    )
    """
    
    upsert_sql = """
    INSERT OR REPLACE INTO matches (
        match_id, match_name, status, venue, runs_team1, runs_team2, last_updated, ingested_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            
            # Prepare records for insertion
            records_to_insert = []
            now_utc = datetime.now(timezone.utc).isoformat()
            
            for _, row in df.iterrows():
                # Handle pandas NaNs/NaTs safely
                r1 = int(row['runs_team1']) if pd.notna(row.get('runs_team1')) else None
                r2 = int(row['runs_team2']) if pd.notna(row.get('runs_team2')) else None
                
                # date column mapped to last_updated
                last_updated = str(row['date']) if pd.notna(row.get('date')) else None
                
                records_to_insert.append((
                    str(row['match_id']),
                    str(row['match_name']) if pd.notna(row.get('match_name')) else None,
                    str(row['status']) if pd.notna(row.get('status')) else None,
                    str(row['venue']) if pd.notna(row.get('venue')) else None,
                    r1,
                    r2,
                    last_updated,
                    now_utc
                ))
            
            # Execute idempotent upsert
            cursor.executemany(upsert_sql, records_to_insert)
            conn.commit()
            
            return len(records_to_insert)
            
    except sqlite3.Error as e:
        logging.error(f"SQLite error loading data: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error loading data into SQLite: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure basic logging for direct execution testing
    logging.basicConfig(level=logging.INFO)
    count = load_data()
    logging.info(f"Loaded {count} matches into DB.")
