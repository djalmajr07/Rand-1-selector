# sampler_logic.py
import pandas as pd
import sqlite3
import math
import logging
from datetime import datetime
import uuid
import os 
import random 

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
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logging.info(f"Created directory for database: {db_dir}")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selection_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
                    status TEXT NOT NULL, message TEXT, selected_count INTEGER NOT NULL,
                    total_in_pool INTEGER NOT NULL, /* Represents total input records processed for this logic */
                    frame_used INTEGER NOT NULL, /* 1 if any input records had a flag, 0 otherwise */
                    log_timestamp DATETIME NOT NULL
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selected_applications (
                    selected_app_id INTEGER PRIMARY KEY AUTOINCREMENT, selection_log_id INTEGER NOT NULL,
                    policy_id TEXT NOT NULL UNIQUE, advisor_id TEXT,
                    sampling_frame_flag INTEGER, application_receive_date TEXT,
                    FOREIGN KEY (selection_log_id) REFERENCES selection_log (log_id)
                )
            ''')
            try:
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_id ON selected_applications (policy_id)')
                logging.info("Ensured UNIQUE index exists on selected_applications.policy_id")
            except sqlite3.Error as e:
                 logging.warning(f"Could not create UNIQUE index on policy_id: {e}")
            conn.commit()
            logging.info(f"Database setup complete or tables already exist: {db_path}")
    except sqlite3.Error as e:
        logging.error(f"Database setup error: {e}")
        raise

def get_previously_selected_policies(db_path):
    """Queries the database and returns a set of all previously selected policy IDs."""
    previously_selected = set()
    try:
        if not os.path.exists(db_path):
             logging.warning(f"Database file not found at {db_path} during get_previously_selected. Returning empty set.")
             return previously_selected
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='selected_applications'")
            if cursor.fetchone():
                cursor.execute("SELECT policy_id FROM selected_applications")
                results = cursor.fetchall()
                previously_selected = {row[0] for row in results}
                logging.info(f"Found {len(previously_selected)} previously selected policy IDs in the database.")
            else:
                logging.warning("'selected_applications' table not found during fetch. Assuming no previous selections.")
    except sqlite3.Error as e:
        logging.error(f"Database error fetching previously selected policies: {e}")
    return previously_selected


def log_attempt(db_path, batch_id, status, message, selected_count, total_input_records, input_contained_flags):
    """Logs the result of a selection attempt to the database."""
    log_timestamp = datetime.now()
    log_id = None
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO selection_log
                (batch_id, status, message, selected_count, total_in_pool, frame_used, log_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (batch_id, status, message, selected_count, total_input_records, 1 if input_contained_flags else 0, log_timestamp))
            log_id = cursor.lastrowid
            conn.commit()
            logging.info(f"Logged attempt: Batch={batch_id}, Status={status}, InputContainedFlags={input_contained_flags}, LogID={log_id}")
            return log_id
    except sqlite3.Error as e:
        logging.error(f"Failed to log attempt for Batch={batch_id}: {e}")
        return None

def save_selected_apps(db_path, selected_df, log_id):
    """Saves the selected applications DataFrame to the database."""
    if selected_df is None or selected_df.empty or log_id is None:
        logging.info("No applications selected or log ID missing, skipping save.")
        return True
    selected_df_to_save = selected_df.copy()
    selected_df_to_save['selection_log_id'] = log_id
    cols_to_keep = [col for col in SELECTED_APP_OUTPUT_COLUMNS if col in selected_df_to_save.columns]
    missing_output_cols = [col for col in SELECTED_APP_OUTPUT_COLUMNS if col not in selected_df_to_save.columns]
    if missing_output_cols:
        logging.error(f"Missing expected columns in DataFrame before saving: {missing_output_cols}. Aborting save.")
        raise ValueError(f"Cannot save selected apps, missing columns: {missing_output_cols}")
    selected_df_to_save = selected_df_to_save[cols_to_keep]
    try:
        with sqlite3.connect(db_path) as conn:
            selected_df_to_save.to_sql('selected_applications', conn, if_exists='append', index=False)
            conn.commit()
            logging.info(f"Successfully saved {len(selected_df_to_save)} selected applications for Log ID {log_id}.")
            return True
    except sqlite3.IntegrityError as e: logging.error(f"Integrity error saving selected apps (duplicate policy_id?): {e}"); raise
    except sqlite3.Error as e: logging.error(f"Failed to save selected applications for Log ID {log_id}: {e}"); raise
    except Exception as e: logging.error(f"An unexpected error occurred during saving selected applications for Log ID {log_id}: {e}"); raise


# --- Main Selection Logic Function ---

def run_selection(input_df, db_path, batch_id):
    """
    Performs per-policy probabilistic (1%) selection with exclusions for flagged
    or previously selected policies.
    """
    status = 'ERROR'
    message = ''
    selected_policies_for_this_batch_rows = [] # Store selected rows (as dicts)
    log_id = None

    # Counters for logging
    num_input_records = 0
    num_passed_1_percent_gate = 0
    num_discarded_due_to_flag_post_gate = 0
    num_discarded_due_to_db_history = 0 # Policies from input already in DB
    num_selected_for_this_batch = 0
    input_contained_any_flags = False # To track for logging

    try:
        # --- 0. Ensure DB Exists and Get Previously Selected Policies ---
        setup_database(db_path) # Ensure DB and tables are ready
        previously_selected_ids_from_db = get_previously_selected_policies(db_path)

        # --- 1. Basic DataFrame Validation ---
        logging.info(f"--- Starting selection for Batch ID: {batch_id} (Per-Policy Logic) ---")
        if not isinstance(input_df, pd.DataFrame) or input_df.empty:
             raise ValueError("Input data must be a non-empty pandas DataFrame.")
        num_input_records = len(input_df) # Get total before any processing
        logging.info(f"Processing {num_input_records} total applications from input DataFrame.")
        all_apps_df = input_df.copy() # Work on a copy

        # --- 2. Column Existence and Initial Type Validation ---
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in all_apps_df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in DataFrame: {', '.join(missing_cols)}")
        for col in REQUIRED_COLUMNS:
             if col in all_apps_df.columns:
                all_apps_df[col] = all_apps_df[col].astype(str).str.strip()
             else:
                 raise ValueError(f"Unexpected missing column during type conversion: {col}")

        # --- 3. Rigorous Data Validation and Type Conversion ---
        logging.info("Performing detailed data validation...")
        validation_errors = []
        # 3a. Policy ID
        if all_apps_df['policy_id'].eq('').any(): validation_errors.append("Empty 'policy_id' found.")
        # 3b. Advisor ID
        if all_apps_df['advisor_id'].eq('').any(): validation_errors.append("Empty 'advisor_id' found.")
        # 3c. Input Duplicates
        if all_apps_df['policy_id'].duplicated().any():
             duplicates = all_apps_df[all_apps_df['policy_id'].duplicated()]['policy_id'].unique()
             validation_errors.append(f"Duplicate policy_id(s) in input: {', '.join(duplicates)}")
        # 3d. Sampling Frame Flag
        try:
            flag_map = {'true': 1, '1': 1, 'yes': 1, 't': 1, 'false': 0, '0': 0, 'no': 0, 'f': 0, '': 0}
            all_apps_df['sampling_frame_flag'] = all_apps_df['sampling_frame_flag'].str.lower().map(flag_map).fillna(0).astype(int)
            if (~all_apps_df['sampling_frame_flag'].isin([0, 1])).any(): validation_errors.append("Invalid 'sampling_frame_flag'.")
            if (all_apps_df['sampling_frame_flag'] == 1).any(): input_contained_any_flags = True # Track if any flags exist in input
        except Exception as e: validation_errors.append(f"Error processing 'sampling_frame_flag': {e}")
        # 3e. Application Receive Date
        try:
            all_apps_df['app_receive_dt_obj'] = pd.to_datetime(all_apps_df['application_receive_date'], errors='coerce')
            invalid_dates = all_apps_df['app_receive_dt_obj'].isnull() & all_apps_df['application_receive_date'].ne('')
            if invalid_dates.any(): validation_errors.append(f"Unparseable 'application_receive_date': {invalid_dates.sum()} rows.")
            today = pd.Timestamp.now().normalize(); min_allowed_date = pd.Timestamp('2000-01-01')
            valid_dates_mask = all_apps_df['app_receive_dt_obj'].notna()
            if valid_dates_mask.any():
                if (all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'] > today).any(): validation_errors.append("Future 'application_receive_date' found.")
                if (all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'] < min_allowed_date).any(): validation_errors.append(f"'application_receive_date' before {min_allowed_date.date()} found.")
            all_apps_df.loc[valid_dates_mask, 'application_receive_date'] = all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'].dt.strftime('%Y-%m-%d')
            all_apps_df.loc[all_apps_df['app_receive_dt_obj'].isnull(), 'application_receive_date'] = ''
        except Exception as e: validation_errors.append(f"Error processing 'application_receive_date': {e}")
        finally:
             if 'app_receive_dt_obj' in all_apps_df.columns: all_apps_df = all_apps_df.drop(columns=['app_receive_dt_obj'])

        if validation_errors:
            error_message = "Input data validation failed:\n- " + "\n- ".join(validation_errors)
            raise ValueError(error_message)
        logging.info("Input data validation successful.")
        # --- End of Validation Block ---

        # --- 4. Per-Policy Selection Logic ---
        logging.info("Applying per-policy probabilistic selection...")
        for index, policy_row in all_apps_df.iterrows():
            policy_id = policy_row['policy_id']
            is_flagged_in_input = policy_row['sampling_frame_flag'] == 1

            # Rule 0: Already selected in a previous run 
            if policy_id in previously_selected_ids_from_db:
                num_discarded_due_to_db_history += 1
                continue # Skip this policy, it's already in the master selected list

            # Rule 1: 1% Initial Chance
            if random.random() >= 0.01: # Fails the 1% chance (random value is in the 99% range)
                continue # Not considered further for selection

            # If here, policy passed the 1% gate
            num_passed_1_percent_gate += 1

            # Rule 2: Flag Exclusion Rule (applied *after* passing 1% gatea)
            if is_flagged_in_input:
                num_discarded_due_to_flag_post_gate += 1
                continue # Discarded due to its flag

            # If I reach here, policy:
            # 1. Is not in the global list of previously selected IDs.
            # 2. Passed the 1% chance.
            # 3. Is NOT flagged.
            # This policy is selected for the current batch.
            selected_policies_for_this_batch_rows.append(policy_row.to_dict())
            num_selected_for_this_batch += 1
            # Add to my set of ever selected IDs to prevent re-selection even within this same batch run
            # if policy_id appeared multiple times (though input validation for duplicates should prevent this).
            # This is crucial for the *next* run of the script.
            previously_selected_ids_from_db.add(policy_id)

        # Create final DataFrame of selected policies for this batch
        final_selected_df = pd.DataFrame(selected_policies_for_this_batch_rows)
        # Ensure columns are in the correct order if DataFrame is not empty
        if not final_selected_df.empty:
            final_selected_df = final_selected_df[REQUIRED_COLUMNS]


        status = 'SUCCESS'
        message = (f"Processed {num_input_records} records. "
                   f"Selected for this batch: {num_selected_for_this_batch}. "
                   f"Passed 1% gate: {num_passed_1_percent_gate}. "
                   f"Discarded (already in DB): {num_discarded_due_to_db_history}. "
                   f"Discarded (flagged post-1% gate): {num_discarded_due_to_flag_post_gate}.")
        logging.info(message)

    except ValueError as e: message = str(e); logging.error(message); status = 'ERROR'
    except TypeError as e: message = str(e); logging.error(message); status = 'ERROR'
    except Exception as e: message = f"An unexpected error occurred: {e}"; logging.exception(message); status = 'ERROR'
    finally:
        # total_in_pool for logging is now num_input_records.
        # frame_used indicates if any flags were present in the input data.
        log_id = log_attempt(db_path, batch_id, status, message, num_selected_for_this_batch, num_input_records, input_contained_any_flags)

        save_successful = False
        if status == 'SUCCESS' and num_selected_for_this_batch > 0 and log_id is not None:
            try:
                save_successful = save_selected_apps(db_path, final_selected_df, log_id)
            except sqlite3.IntegrityError as ie:
                 status = 'ERROR'; message = f"IntegrityError: Failed to save (duplicate policy?). Selected: {num_selected_for_this_batch}"; logging.error(f"DB INTEGRITY ERROR: Batch={batch_id}, LogID={log_id}: {ie}")
            except Exception as e:
                 status = 'ERROR'; message = f"Failed to save apps: {e}"; logging.error(f"CRITICAL: Failed to save apps for log_id {log_id}: {e}")

            if status == 'ERROR' and log_id is not None: # If save failed, update the log entry
                 try:
                    with sqlite3.connect(db_path) as conn: conn.execute("UPDATE selection_log SET status = ?, message = ? WHERE log_id = ?", (status, message, log_id)); conn.commit(); logging.info(f"Updated log {log_id} status to ERROR due to save failure.")
                 except Exception as update_e: logging.error(f"Failed to update log status for {log_id}: {update_e}")
        elif status == 'ERROR':
             logging.warning(f"Batch {batch_id} finished with ERROR during selection phase. No apps saved.")
    return log_id is not None
