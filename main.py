import sys
import logging
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

def setup_logging():
    """
    Configures logging to output to both pipeline.log and stdout at the INFO level.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Avoid adding duplicate handlers if re-run in an interactive session
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # File handler
        file_handler = logging.FileHandler('pipeline.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

def main():
    """
    Orchestrates the Cricket Live Data ETL Pipeline.
    Runs extract, transform, and load sequentially with isolated error handling.
    """
    setup_logging()
    logging.info("Starting Cricket Live Data ETL Pipeline")
    
    # --- STAGE 1: EXTRACT ---
    logging.info("--- Stage 1: Extraction ---")
    try:
        extracted_count = extract_data()
        logging.info(f"Extracted {extracted_count} matches")
    except Exception as e:
        logging.error("Pipeline failed during EXTRACTION stage.")
        logging.exception(e)  # Logs the full traceback
        sys.exit(1)

    # --- STAGE 2: TRANSFORM ---
    logging.info("--- Stage 2: Transformation ---")
    try:
        transformed_count = transform_data()
        logging.info(f"Transformed {transformed_count} matches")
    except Exception as e:
        logging.error("Pipeline failed during TRANSFORMATION stage.")
        logging.exception(e)
        sys.exit(1)

    # --- STAGE 3: LOAD ---
    logging.info("--- Stage 3: Loading ---")
    try:
        loaded_count = load_data()
        logging.info(f"Loaded {loaded_count} matches into DB")
    except Exception as e:
        logging.error("Pipeline failed during LOADING stage.")
        logging.exception(e)
        sys.exit(1)
        
    logging.info("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()
