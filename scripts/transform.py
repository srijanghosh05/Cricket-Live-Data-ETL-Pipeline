import re
import sys
import json
import logging
import pandas as pd
from pathlib import Path

def parse_runs(score_str: str) -> int:
    """
    Extracts the runs integer from a score string (e.g., '250/4 (50)', '250', or '250/2 d')
    using regex. Returns None if no digits are found.
    """
    if not isinstance(score_str, str):
        return None
        
    match = re.search(r'(\d+)', score_str)
    if match:
        return int(match.group(1))
    return None

def transform_data() -> int:
    """
    Reads the most recent raw JSON from the Bronze layer, flattens the nested 
    structure, extracts team scores, drops invalid rows, and saves as CSV in 
    the Silver layer.
    Returns the number of rows transformed.
    """
    project_root = Path(__file__).resolve().parent.parent
    bronze_dir = project_root / "data" / "bronze"
    silver_dir = project_root / "data" / "silver"
    
    # Ensure silver directory exists
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # Reliable File Discovery: Get the most recent JSON file by creation time
    json_files = list(bronze_dir.glob("*.json"))
    if not json_files:
        logging.error("No JSON files found in data/bronze/")
        sys.exit(1)
        
    latest_file = max(json_files, key=lambda f: f.stat().st_ctime)
    
    try:
        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        matches = data.get("data", [])
        if not matches:
            logging.warning(f"No match data found in {latest_file.name}")
            return 0
            
        transformed_records = []
        for match in matches:
            # Drop records missing fundamental IDs or names
            if not match.get("id") or not match.get("name"):
                continue
                
            record = {
                "match_id": str(match.get("id")),
                "match_name": match.get("name"),
                "status": match.get("status"),
                "venue": match.get("venue"),
                "matchType": match.get("matchType"),
                "date": match.get("date"),
                "runs_team1": None,
                "runs_team2": None
            }
            
            # Handle nested score structure
            scores = match.get("score", [])
            if scores and isinstance(scores, list):
                if len(scores) > 0:
                    record["runs_team1"] = parse_runs(scores[0].get("r", "")) or parse_runs(str(scores[0].get("inning", "")))
                if len(scores) > 1:
                    record["runs_team2"] = parse_runs(scores[1].get("r", "")) or parse_runs(str(scores[1].get("inning", "")))
                    
            transformed_records.append(record)
            
        df = pd.DataFrame(transformed_records)
        
        # Save to Silver layer
        output_path = silver_dir / "match_updates.csv"
        df.to_csv(output_path, index=False)
        
        return len(df)
        
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure basic logging for direct execution testing
    logging.basicConfig(level=logging.INFO)
    count = transform_data()
    logging.info(f"Transformed {count} matches.")
