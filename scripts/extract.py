import os
import sys
import json
import logging
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

def extract_data() -> int:
    """
    Fetches live cricket match data from CricketData.org API and saves it 
    as a raw JSON file in the Bronze layer.
    Returns the number of matches extracted.
    """
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("CRICKETDATA_API_KEY")
    
    if not api_key:
        logging.error("CRICKETDATA_API_KEY is not set in the environment.")
        sys.exit(1)

    url = f"https://api.cricapi.com/v1/currentMatches?apikey={api_key}&offset=0"
    
    # Ensure bronze directory exists using absolute pathing for EC2 compatibility
    project_root = Path(__file__).resolve().parent.parent
    bronze_dir = project_root / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        
        data = response.json()
        
        if data.get("status") != "success":
            logging.error(f"API returned non-success status: {data.get('reason', 'Unknown error')}")
            sys.exit(1)

        matches = data.get("data", [])
        
        # Save raw JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filepath = bronze_dir / f"matches_{timestamp}.json"
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
            
        return len(matches)
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting data from API: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure basic logging for direct execution testing
    logging.basicConfig(level=logging.INFO)
    count = extract_data()
    logging.info(f"Extracted {count} matches.")
