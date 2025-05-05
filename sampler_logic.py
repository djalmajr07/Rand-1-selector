import pandas as pd
import sqlite3
import math
import logging
from datetime import datetime
import uuid # For potential UUID validation
import os # Needed for setup_database check

# --- Configuration ---
# Define required columns for the input data
REQUIRED_COLUMNS = [
    'policy_id', 'application_receive_date', 'advisor_id',
    'branch_name', 'sampling_frame_flag'
]
# Define columns for the selected applications output table
SELECTED_APP_OUTPUT_COLUMNS = [
    'selection_log_id', 'policy_id', 'advisor_id',
    'sampling_frame_flag', 'application_receive_date'
]

# --- Database Functions ---

def setup_database(db_path):
    """Creates the necessary SQLite tables if they don't exist."""
    try:
        # Ensure directory exists if db_path includes directories
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logging.info(f"Created directory for database: {db_dir}")

        # Connect to the database, creating it if it doesn't exist
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Create selection_log table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selection_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
                    status TEXT NOT NULL, message TEXT, selected_count INTEGER NOT NULL,
                    total_in_pool INTEGER NOT NULL, frame_used INTEGER NOT NULL,
                    log_timestamp DATETIME NOT NULL
                )
            ''')
            # Create selected_applications table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selected_applications (
                    selected_app_id INTEGER PRIMARY KEY AUTOINCREMENT, selection_log_id INTEGER NOT NULL,
                    policy_id TEXT NOT NULL UNIQUE, advisor_id TEXT,
                    sampling_frame_flag INTEGER, application_receive_date TEXT,
                    FOREIGN KEY (selection_log_id) REFERENCES selection_log (log_id)
                )
            ''')
            # When change it for Equitable system this part will need to be changed or removed
            # Here's where I ensurance the policy_id is unique in the selected_applications table
            # Attempt to create a UNIQUE index on policy_id for data integrity
            # Use IF NOT EXISTS to avoid errors if the index already exists
            try:
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_id ON selected_applications (policy_id)')
                logging.info("Ensured UNIQUE index exists on selected_applications.policy_id")
            except sqlite3.Error as e:
                 # Log a warning if index creation fails (might be due to existing duplicates if run on old data)
                 logging.warning(f"Could not create UNIQUE index on policy_id: {e}")
            # Commit changes
            conn.commit()
            logging.info(f"Database setup complete or tables already exist: {db_path}")
    except sqlite3.Error as e:
        # Log and re-raise database errors during setup
        logging.error(f"Database setup error: {e}")
        raise

def get_previously_selected_policies(db_path):
    """Queries the database and returns a set of all previously selected policy IDs."""
    previously_selected = set()
    try:
        # Ensure DB exists before querying 
        if not os.path.exists(db_path):
             logging.warning(f"Database file not found at {db_path} during get_previously_selected. Returning empty set.")
             return previously_selected # Return empty set if DB file doesn't exist

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Check if table exists first for robustness
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='selected_applications'")
            if cursor.fetchone():
                # Fetch all policy IDs from the table
                cursor.execute("SELECT policy_id FROM selected_applications")
                results = cursor.fetchall()
                # Add fetched IDs to a set for efficient lookup
                previously_selected = {row[0] for row in results}
                logging.info(f"Found {len(previously_selected)} previously selected policy IDs in the database.")
            else:
                # This case indicates the table is missing even though the DB file exists
                logging.warning("'selected_applications' table not found during fetch. Assuming no previous selections.")
    except sqlite3.Error as e:
        logging.error(f"Database error fetching previously selected policies: {e}")
        # Return empty set on error, allowing process to potentially continue
    return previously_selected


def log_attempt(db_path, batch_id, status, message, selected_count, total_in_pool, frame_mode_active):
    """Logs the result of a selection attempt to the database."""
    log_timestamp = datetime.now()
    log_id = None
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Insert log record
            cursor.execute('''
                INSERT INTO selection_log
                (batch_id, status, message, selected_count, total_in_pool, frame_used, log_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (batch_id, status, message, selected_count, total_in_pool, 1 if frame_mode_active else 0, log_timestamp))
            # Get the ID of the inserted log record
            log_id = cursor.lastrowid
            conn.commit()
            logging.info(f"Logged attempt: Batch={batch_id}, Status={status}, FrameMode={frame_mode_active}, LogID={log_id}")
            return log_id
    except sqlite3.Error as e:
        logging.error(f"Failed to log attempt for Batch={batch_id}: {e}")
        return None # Indicate logging failure

def save_selected_apps(db_path, selected_df, log_id):
    """Saves the selected applications DataFrame to the database."""
    # Skip if DataFrame is empty or log ID is missing
    if selected_df is None or selected_df.empty or log_id is None:
        logging.info("No applications selected or log ID missing, skipping save.")
        return True # Indicate success (nothing to save)

    # Prepare DataFrame for saving
    selected_df_to_save = selected_df.copy()
    selected_df_to_save['selection_log_id'] = log_id

    # Ensure only the required columns are present
    cols_to_keep = [col for col in SELECTED_APP_OUTPUT_COLUMNS if col in selected_df_to_save.columns]
    missing_output_cols = [col for col in SELECTED_APP_OUTPUT_COLUMNS if col not in selected_df_to_save.columns]
    if missing_output_cols:
        logging.error(f"Missing expected columns in DataFrame before saving: {missing_output_cols}. Aborting save.")
        raise ValueError(f"Cannot save selected apps, missing columns: {missing_output_cols}")

    selected_df_to_save = selected_df_to_save[cols_to_keep]

    try:
        # Connect and save DataFrame to SQL table
        with sqlite3.connect(db_path) as conn:
            selected_df_to_save.to_sql(
                'selected_applications', conn, if_exists='append', index=False
            )
            conn.commit()
            logging.info(f"Successfully saved {len(selected_df_to_save)} selected applications for Log ID {log_id}.")
            return True # Indicate success
    except sqlite3.IntegrityError as e:
         # Catch potential duplicate key errors due to UNIQUE constraint
         logging.error(f"Integrity error saving selected apps (duplicate policy_id!!!!): {e}")
         raise # Re-raise to be handled by the caller's error handling
    except sqlite3.Error as e:
        logging.error(f"Failed to save selected applications for Log ID {log_id}: {e}")
        raise # Re-raise other SQLite errors # If i remember something else just add it here
    except Exception as e:
         logging.error(f"An unexpected error occurred during saving selected applications for Log ID {log_id}: {e}")
         raise # Re-raise unexpected errors


# --- Main Selection Logic Function (with Enhanced Validation) ---

def run_selection(input_df, db_path, batch_id):
    """
    Performs random selection, auto-detecting frame usage based on input data
    and excluding previously selected policies. Includes enhanced validation.

    Args:
        input_df (pd.DataFrame): DataFrame containing application data. Must include columns defined in REQUIRED_COLUMNS.
        db_path (str): Path to the SQLite database file.
        batch_id (str): A unique identifier for this selection batch.

    Returns:
        bool: True if the process completed logging stage, False otherwise.
        Note: Final status (SUCCESS/ERROR) is in the log.
    """
    # Initialize variables
    status = 'ERROR'; message = ''; selected_count = 0; total_in_pool = 0
    final_selected_df = pd.DataFrame(); log_id = None; frame_mode_active = False

    try:
        # --- 0. Ensure DB Exists and Get Previously Selected Policies ---
        setup_database(db_path) # Ensure DB and tables are ready
        previously_selected_ids = get_previously_selected_policies(db_path)

        # --- 1. Basic DataFrame Validation ---
        logging.info(f"--- Starting selection for Batch ID: {batch_id} ---")
        if not isinstance(input_df, pd.DataFrame) or input_df.empty:
             raise ValueError("Input data can't be empty.")
        total_applications_read = len(input_df)
        logging.info(f"Processing {total_applications_read} total applications from input DataFrame.")
        all_apps_df = input_df.copy() 

        # --- 2. Column Existence and Initial Type Validation ---
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in all_apps_df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in Dataframe: {', '.join(missing_cols)}")
        # Convert all relevant columns to string initially, stripping whitespace
        # In order to handle EQ policies this is a must, several contains blank after the last number
        for col in REQUIRED_COLUMNS:
             # Ensure column exists before trying to convert
             if col in all_apps_df.columns:
                all_apps_df[col] = all_apps_df[col].astype(str).str.strip()
             else:
                 # This case should be caught by missing_cols check, but as safety this a double check
                 raise ValueError(f"Unexpected missing column during type conversion: {col}")


        # --- 3. Rigorous Data Validation and Type Conversion ---
        logging.info("Performing detailed data validation...")
        validation_errors = []

        # 3a. Policy ID: Check non-empty
        if all_apps_df['policy_id'].eq('').any():
            validation_errors.append("Found rows with empty 'policy_id'.")
        # Optional: Add more specific format checks if needed (e.g., length, pattern)

        # 3b. Advisor ID: Check non-empty
        if all_apps_df['advisor_id'].eq('').any():
            validation_errors.append("Found rows with empty 'advisor_id'.")

        # 3c. Input Duplicates: Check for duplicate policy_ids WITHIN the input file
        if all_apps_df['policy_id'].duplicated().any():
             duplicates = all_apps_df[all_apps_df['policy_id'].duplicated()]['policy_id'].unique()
             validation_errors.append(f"Duplicate policy_id(s) found within the input file: {', '.join(duplicates)}")

        # 3d. Sampling Frame Flag: Convert and ensure 0 or 1
        # Check how the flag is represented over EQ xml files
        try:
            flag_map = {'true': 1, '1': 1, 'yes': 1, 't': 1,
                        'false': 0, '0': 0, 'no': 0, 'f': 0, '': 0} # Treat empty as 0
            # Convert to lower case, map, fill unmappable with 0, ensure int
            all_apps_df['sampling_frame_flag'] = all_apps_df['sampling_frame_flag'].str.lower().map(flag_map).fillna(0).astype(int)
            # This check is redundant due to fillna(0) and astype(int) but kept for clarity
            invalid_flags = ~all_apps_df['sampling_frame_flag'].isin([0, 1])
            if invalid_flags.any():
                 validation_errors.append("Found invalid values in 'sampling_frame_flag' (should be convertible to 0 or 1).")
        except Exception as e:
            validation_errors.append(f"Error processing 'sampling_frame_flag': {e}")

        # 3e. Application Receive Date: Convert, check format, check range
        try:
            # Convert to datetime objects, coercing errors to NaT
            all_apps_df['app_receive_dt_obj'] = pd.to_datetime(all_apps_df['application_receive_date'], errors='coerce')
            # Check only non-empty strings that failed parse
            invalid_dates = all_apps_df['app_receive_dt_obj'].isnull() & all_apps_df['application_receive_date'].ne('')
            if invalid_dates.any():
                validation_errors.append(f"Found {invalid_dates.sum()} rows with unparseable, non-empty 'application_receive_date'.")

            today = pd.Timestamp.now().normalize()
            min_allowed_date = pd.Timestamp('2000-01-01') # Example minimum date
            # Check only on valid dates
            valid_dates_mask = all_apps_df['app_receive_dt_obj'].notna()
            if valid_dates_mask.any(): # Proceed only if there are some valid dates
                future_dates = all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'] > today
                too_old_dates = all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'] < min_allowed_date
                if future_dates.any():
                     validation_errors.append(f"Found {future_dates.sum()} rows with future 'application_receive_date'.")
                if too_old_dates.any():
                     validation_errors.append(f"Found {too_old_dates.sum()} rows with 'application_receive_date' before {min_allowed_date.date()}.")

            # Convert valid dates back to YYYY-MM-DD string
            # Use .loc to avoid SettingWithCopyWarning
            all_apps_df.loc[valid_dates_mask, 'application_receive_date'] = all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'].dt.strftime('%Y-%m-%d')
            # Set invalid/empty original dates to empty string in final column
            all_apps_df.loc[all_apps_df['app_receive_dt_obj'].isnull(), 'application_receive_date'] = ''

        except Exception as e:
            validation_errors.append(f"Error processing 'application_receive_date': {e}")
        finally:
             # Drop the temporary datetime object column if it exists
             if 'app_receive_dt_obj' in all_apps_df.columns:
                 all_apps_df = all_apps_df.drop(columns=['app_receive_dt_obj'])


        # --- Handle Validation Errors ---
        if validation_errors:
            # Combine error messages into a single string
            error_message = "Input data validation failed:\n- " + "\n- ".join(validation_errors)
            logging.error(error_message)
            raise ValueError(error_message) # Stop processing
        else:
            logging.info("Input data validation successful.")


        # --- 4. Auto-Detect Frame Mode ---
        # Check if any flags are set to 1
        if (all_apps_df['sampling_frame_flag'] == 1).any():
            frame_mode_active = True; logging.info("Flagged apps detected. Activating frame mode.")
            # Filter for flagged applications
            initial_candidate_pool_df = all_apps_df[all_apps_df['sampling_frame_flag'] == 1].copy()
        else:
            frame_mode_active = False; logging.info("No flagged apps detected. Using all.")
            # Use all applications
            initial_candidate_pool_df = all_apps_df.copy()
        logging.info(f"Initial pool size (based on frame): {len(initial_candidate_pool_df)}")

        # --- 5. Exclude Previously Selected Policies ---
        if not previously_selected_ids:
            logging.info("No previously selected policies found. Using initial pool.")
            eligible_candidate_pool_df = initial_candidate_pool_df
        else:
            logging.info(f"Excluding up to {len(previously_selected_ids)} previously selected policies.") #call my fucn from line 186
            # Filter out policies whose IDs are in the previously_selected set
            eligible_candidate_pool_df = initial_candidate_pool_df[~initial_candidate_pool_df['policy_id'].isin(previously_selected_ids)].copy()
            excluded_count = len(initial_candidate_pool_df) - len(eligible_candidate_pool_df)
            logging.info(f"Excluded {excluded_count} policies found in previous selections.")
        # Calculate the size of the final pool after exclusions
        total_in_pool = len(eligible_candidate_pool_df)
        logging.info(f"Final eligible candidate pool size: {total_in_pool}")

        # --- 6. Calculate Target Sample Size ---
        target_sample_size = 0
        if total_in_pool > 0:
            # Calculate 1% target, ensuring at least 1 is selected if pool is non-empty
            # Not sure if this is the best way to do it, I would like to hear from the business side
            target_sample_size = max(1, math.floor(0.01 * total_in_pool))
            logging.info(f"Pool size (after exclusions): {total_in_pool}. Target: {target_sample_size}")
        else:
            logging.info("Eligible pool empty. Target is 0.")
        # Ensure target doesn't exceed available pool size
        if target_sample_size > total_in_pool:
            target_sample_size = total_in_pool; logging.warning(f"Adjusting target to pool size: {target_sample_size}")

        # --- 7. Perform Random Selection ---
        selection_description = "flagged" if frame_mode_active else "total (no flags found)"
        if target_sample_size > 0 and not eligible_candidate_pool_df.empty:
            logging.info(f"Selecting {target_sample_size} applications randomly.")
            # Perform random sampling without replacement
            final_selected_df = eligible_candidate_pool_df.sample(n=target_sample_size, random_state=None, replace=False)
            selected_count = len(final_selected_df)
            logging.info(f"Successfully selected {selected_count} applications.")
            status = 'SUCCESS'
            message = f"Selected {selected_count} {selection_description} from {total_in_pool} eligible (after exclusions)."
        else:
            # Handle cases where target is 0 or pool is empty
            logging.info("No applications selected."); status = 'SUCCESS'
            message = f"Selected 0 {selection_description} from {total_in_pool} eligible (after exclusions)."
            selected_count = 0; final_selected_df = pd.DataFrame() # Ensure empty DataFrame

    # --- Catch specific expected errors ---
    except ValueError as e: # Catch validation errors or other ValueErrors
        message = str(e); logging.error(message); status = 'ERROR'
    except TypeError as e: # Catch potential type errors during processing
        message = str(e); logging.error(message); status = 'ERROR'
    # --- Catch any other unexpected errors ---
    except Exception as e:
        message = f"An unexpected error occurred during selection: {e}"; logging.exception(message); status = 'ERROR'
    finally:
        # --- 8. Log Result & Attempt to Save ---
        # Ensure logging happens even if selection failed
        log_id = log_attempt(db_path, batch_id, status, message, selected_count, total_in_pool, frame_mode_active)

        save_successful = False
        # Only attempt save if selection was initially marked SUCCESSFUL and items were selected
        if status == 'SUCCESS' and selected_count > 0 and log_id is not None:
            try:
                # Attempt to save the selected applications
                save_successful = save_selected_apps(db_path, final_selected_df, log_id)
                # If save_selected_apps itself indicated failure (though it raises errors now)
                if not save_successful:
                     status = 'ERROR'
                     message = "Save operation failed without specific error." # Should not happen
            except sqlite3.IntegrityError as ie:
                 # Handle duplicate key error specifically
                 status = 'ERROR'; message = f"IntegrityError: Failed to save (duplicate policy?). Count: {selected_count}"; logging.error(f"DB INTEGRITY ERROR: Batch={batch_id}, LogID={log_id}: {ie}")
            except Exception as e:
                 # Handle any other error during save
                 status = 'ERROR'; message = f"Failed to save apps: {e}"; logging.error(f"CRITICAL: Failed to save apps for log_id {log_id}: {e}")

            # If save failed (status is now ERROR), update the log entry
            if status == 'ERROR' and log_id is not None:
                 try:
                    with sqlite3.connect(db_path) as conn: conn.execute("UPDATE selection_log SET status = ?, message = ? WHERE log_id = ?", (status, message, log_id)); conn.commit(); logging.info(f"Updated log {log_id} status to ERROR due to save failure.")
                 except Exception as update_e: logging.error(f"Failed to update log status for {log_id}: {update_e}")

        elif status == 'ERROR':
             # Log if selection phase itself failed
             logging.warning(f"Batch {batch_id} finished with ERROR during selection phase. No apps saved.")

    # Return True if the process reached the logging stage, False otherwise
    return log_id is not None

