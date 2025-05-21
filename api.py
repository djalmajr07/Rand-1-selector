# main.py
import argparse
import pandas as pd
import logging
import sys
import os
import configparser
from sampler_logic import run_selection, REQUIRED_INPUT_COLUMNS

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Function to read config ---
def get_config_values(config_path='config.ini'):
    """Reads configuration from the specified INI file."""
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    try:
        db_path = config.get('Database', 'db_path', fallback='sampler_data_v2.db')
        target_overall_rate = config.getfloat('Sampling', 'target_overall_selection_rate', fallback=0.01)
        estimated_unprotected_ratio = config.getfloat('Sampling', 'estimated_unprotected_ratio', fallback=0.80)
        
        if not (0 < target_overall_rate <= 1):
            raise ValueError("target_overall_selection_rate must be between 0 (exclusive) and 1 (inclusive).")
        if not (0 < estimated_unprotected_ratio <= 1):
            raise ValueError("estimated_unprotected_ratio must be between 0 (exclusive) and 1 (inclusive).")

        return db_path, target_overall_rate, estimated_unprotected_ratio
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        logging.error(f"Configuration error in '{config_path}': {e}")
        raise # Re-raise to be caught by main

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description='Run the per-policy application selection process.')
    parser.add_argument('--input-csv', required=True, help='Path to the input CSV file (columns: id, protected_class, xml_blob, ...).')
    parser.add_argument('--db-path', required=False, default=None, help='(Optional) Override database path from config.ini.')
    parser.add_argument('--batch-id', required=True, help='A unique identifier for this selection batch.')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file.')

    args = parser.parse_args()

    # --- Read Configuration ---
    try:
        db_path_conf, target_rate_conf, est_unprotected_ratio_conf = get_config_values(args.config)
        db_path = args.db_path if args.db_path else db_path_conf # Command line overrides config
    except Exception as e: # Catches FileNotFoundError or config errors from get_config_values
        sys.exit(1)

    logging.info(f"Starting main script for Batch ID: {args.batch_id}")
    logging.info(f"Input CSV: {args.input_csv}")
    logging.info(f"Database Path: {db_path}")
    logging.info(f"Using Config File: {args.config}")
    logging.info(f"Target Overall Selection Rate from Config: {target_rate_conf*100:.2f}%")
    logging.info(f"Estimated Unprotected Ratio from Config: {est_unprotected_ratio_conf*100:.2f}%")

    # --- Read and Validate Input CSV ---
    try:
        if not os.path.exists(args.input_csv):
            raise FileNotFoundError(f"Input CSV file not found: {args.input_csv}")
        
        # Read all columns as string initially to handle various inputs
        input_df = pd.read_csv(args.input_csv, dtype=str, keep_default_na=False)
        logging.info(f"Successfully read {len(input_df)} records from {args.input_csv}")

        if input_df.empty:
             raise pd.errors.EmptyDataError("Input CSV file is empty.")

        # Check for new REQUIRED_INPUT_COLUMNS
        missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in input_df.columns]
        if missing_cols:
             raise ValueError(f"Input CSV is missing required columns: {', '.join(missing_cols)}")

    except FileNotFoundError as e: logging.error(str(e)); sys.exit(1)
    except pd.errors.EmptyDataError as e: logging.error(str(e)); sys.exit(1)
    except ValueError as e: logging.error(f"Input CSV column error: {e}"); sys.exit(1)
    except Exception as e:
        logging.error(f"Failed to read/validate input CSV: {e}"); logging.exception("Details:"); sys.exit(1)

    # --- Run Selection Process ---
    try:
        # Pass the new config values to run_selection
        completed_logging = run_selection(input_df, db_path, args.batch_id,
                                          target_rate_conf, est_unprotected_ratio_conf)
        if completed_logging:
            logging.info(f"Selection process completed for Batch ID: {args.batch_id}.")
            sys.exit(0)
        else:
            logging.error(f"Selection process failed critically before logging for Batch ID: {args.batch_id}.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error during selection process: {e}"); logging.exception("Details:"); sys.exit(1)

if __name__ == '__main__':
    main()
