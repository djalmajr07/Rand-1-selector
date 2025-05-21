# main.py
import argparse
import pandas as pd
import logging
import sys
import os
import configparser
from datetime import date # For default date
# Import the core function and NEW required columns list
from sampler_logic import run_selection, REQUIRED_INPUT_COLUMNS

# --- Configuration ---
# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Function to read config ---
def get_config_values(config_path='config.ini'):
    """Reads configuration from the specified INI file."""
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        # Raise specific error to be caught in main
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
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(description='Run the per-policy application selection process.')
    parser.add_argument('--input-file', required=True, help='Path to the input data file (CSV or TSV).')
    parser.add_argument('--db-path', required=False, default=None, help='(Optional) Override database path from config.ini.')
    parser.add_argument('--batch-id', required=True, help='A unique identifier for this selection batch.')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file.')
    parser.add_argument('--delimiter', default=None, help='(Optional) Delimiter for the input file (e.g., "," for CSV, "\\t" for TSV). Auto-detect if not provided.')


    args = parser.parse_args()

    # --- Read Configuration ---
    try:
        db_path_conf, target_rate_conf, est_unprotected_ratio_conf = get_config_values(args.config)
        db_path = args.db_path if args.db_path else db_path_conf # Command line overrides config
    except Exception as e: # Catches FileNotFoundError or config errors from get_config_values
        sys.exit(1)

    # Log initial parameters
    logging.info(f"Starting main script for Batch ID: {args.batch_id}")
    logging.info(f"Input File: {args.input_file}")
    logging.info(f"Database Path: {db_path}")
    logging.info(f"Using Config File: {args.config}")
    logging.info(f"Target Overall Selection Rate from Config: {target_rate_conf*100:.2f}%")
    logging.info(f"Estimated Unprotected Ratio from Config: {est_unprotected_ratio_conf*100:.2f}%")


    # --- Read and Validate Input File ---
    try:
        # Check if input file exists
        if not os.path.exists(args.input_file):
            raise FileNotFoundError(f"Input file not found: {args.input_file}")

        # Read CSV/TSV, treating all as strings initially
        # Use specified delimiter or let pandas infer if None
        delimiter_to_use = args.delimiter
        if delimiter_to_use == "\\t": # Handle escaped tab character from command line
            delimiter_to_use = "\t"

        input_df = pd.read_csv(
            args.input_file,
            dtype=str,
            keep_default_na=False,
            delimiter=delimiter_to_use, # Pass the delimiter
            engine='python' # 'python' engine is more flexible with delimiters and quoting
        )
        logging.info(f"Successfully read {len(input_df)} records from {args.input_file}")

        if input_df.empty:
             raise pd.errors.EmptyDataError("Input file is empty.")

        # --- Add missing contextual columns with placeholders if they don't exist ---
        # These are expected by sampler_logic.py for validation and storage,
        # even if not directly used in the selection decision for this specific input.
        default_date = date.today().strftime('%Y-%m-%d')
        placeholder_text = "N/A_FROM_RAW_INPUT"

        if 'application_receive_date' not in input_df.columns:
            input_df['application_receive_date'] = default_date
            logging.info(f"Added missing 'application_receive_date' column with default: {default_date}")
        if 'advisor_id' not in input_df.columns:
            input_df['advisor_id'] = placeholder_text
            logging.info(f"Added missing 'advisor_id' column with placeholder: {placeholder_text}")
        if 'branch_name' not in input_df.columns:
            input_df['branch_name'] = placeholder_text
            logging.info(f"Added missing 'branch_name' column with placeholder: {placeholder_text}")
        # Ensure 'xml_blob' exists, add if missing (though sample has it)
        if 'xml_blob' not in input_df.columns:
            input_df['xml_blob'] = "" # Add empty string if missing
            logging.info("Added missing 'xml_blob' column with empty string.")


        # Check for new REQUIRED_INPUT_COLUMNS
        missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in input_df.columns]
        if missing_cols:
             raise ValueError(f"Input file is missing required columns after attempting to add placeholders: {', '.join(missing_cols)}")

    except FileNotFoundError as e: logging.error(str(e)); sys.exit(1)
    except pd.errors.EmptyDataError as e: logging.error(str(e)); sys.exit(1)
    except ValueError as e: logging.error(f"Input file column error: {e}"); sys.exit(1)
    except Exception as e:
        logging.error(f"Failed to read/validate input file: {e}"); logging.exception("Details:"); sys.exit(1)

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
