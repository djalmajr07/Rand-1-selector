import argparse
import pandas as pd
import logging
import sys
import os
import configparser # For reading INI files
from sampler_logic import run_selection, REQUIRED_COLUMNS 

#  Configuration 
# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#  Function to read config 
def get_config(config_path='config.ini'):
    """Reads configuration from the specified INI file."""
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        # Raise specific error to be caught in main
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

#  Main Execution 
def main():
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(description='Run the application selection process.')
    parser.add_argument('--input-csv', required=True, help='Path to the input CSV file.')
    parser.add_argument('--db-path', required=False, default=None,
                        help='(Optional) Override database path specified in config.ini.')
    parser.add_argument('--batch-id', required=True, help='A unique identifier for this selection batch.')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file.')

    args = parser.parse_args()

    #  Read Configuration 
    try:
        config = get_config(args.config)
        # Determine database path: command line > config file > default fallback
        db_path = args.db_path if args.db_path else config.get('Database', 'db_path', fallback='sampler_data.db') 
    except FileNotFoundError:
        # Error already logged in get_config
        sys.exit(1) # Exit if config file not found
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logging.error(f"Configuration error in '{args.config}': Missing section or option - {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading configuration file '{args.config}': {e}")
        sys.exit(1)

    # Log initial parameters
    logging.info(f"Starting main script for Batch ID: {args.batch_id}")
    logging.info(f"Input CSV: {args.input_csv}")
    logging.info(f"Database Path: {db_path} (from {'command line' if args.db_path else 'config file'})")
    logging.info(f"Using Config File: {args.config}")


    #  Read and Validate Input CSV 
    try:
        # Check if input file exists
        if not os.path.exists(args.input_csv):
            raise FileNotFoundError(f"Input CSV file not found: {args.input_csv}")

        # Read CSV, treating all as strings initially to handle various inputs robustly
        input_df = pd.read_csv(args.input_csv, dtype=str, keep_default_na=False)
        logging.info(f"Successfully read {len(input_df)} records from {args.input_csv}")

        # Check for empty file after reading
        if input_df.empty:
             # Raise specific error for empty data, caught below
             raise pd.errors.EmptyDataError("Input CSV file is empty.")

        # Basic column existence check (detailed validation happens in run_selection)
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in input_df.columns]
        if missing_cols:
             raise ValueError(f"The CSV is missing the following required columns: {', '.join(missing_cols)}")

    # Handle specific file/data errors
    except FileNotFoundError as e: logging.error(str(e)); sys.exit(1)
    except pd.errors.EmptyDataError as e: logging.error(str(e)); sys.exit(1)
    except ValueError as e: logging.error(f"Input CSV column error: {e}"); sys.exit(1)
    # Handle any other potential errors during read/basic validation
    except Exception as e:
        logging.error(f"Failed to read or perform basic validation on input CSV: {e}")
        logging.exception("Details:") # Log traceback for unexpected errors
        sys.exit(1)

    #  Run Selection Process 
    try:
        # Call the main logic function from the imported module (sampler_logic)
        completed_logging = run_selection(input_df, db_path, args.batch_id)

        # Check if the process reached the logging stage
        if completed_logging:
            logging.info(f"Selection process completed for Batch ID: {args.batch_id}. Check logs and database for final status.")
            # Exit code 0 signifies the script ran, but check logs for SUCCESS/ERROR status
            sys.exit(0)
        else:
            # This path indicates a failure before logging could occur
            logging.error(f"Selection process failed critically before logging for Batch ID: {args.batch_id}.")
            sys.exit(1)

    except Exception as e:
        # Catch any unexpected errors bubbling up from run_selection
        logging.error(f"An unexpected error occurred during the main selection process: {e}")
        logging.exception("Details:")
        sys.exit(1) # Exit with error code


if __name__ == '__main__':
    main()
