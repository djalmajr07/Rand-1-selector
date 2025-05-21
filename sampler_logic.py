# # sampler_logic.py
# import pandas as pd
# import sqlite3
# import math
# import logging
# from datetime import datetime
# import uuid
# import os
# import random

# # --- Configuration ---
# # Define required columns for the INPUT data based on new requirements
# REQUIRED_INPUT_COLUMNS = [
#     'id', 'protected_class', 'xml_blob', # Core for new logic
#     'application_receive_date', 'advisor_id', 'branch_name' # Contextual, for storage
# ]
# # Define columns for the selected_applications DATABASE table
# # 'policy_id' in DB maps to 'id' from input.
# # 'sampling_frame_flag' is no longer stored for selected items as they are, by definition, not protected.
# SELECTED_APP_DB_COLUMNS = [
#     'selection_log_id', 'policy_id', 'advisor_id',
#     'application_receive_date'
# ]

# # --- Database Functions ---

# def setup_database(db_path):
#     """Creates the necessary SQLite tables if they don't exist."""
#     try:
#         db_dir = os.path.dirname(db_path)
#         if db_dir and not os.path.exists(db_dir):
#             os.makedirs(db_dir); logging.info(f"Created directory for database: {db_dir}")
#         with sqlite3.connect(db_path) as conn:
#             cursor = conn.cursor()
#             # Log table: frame_used now stores estimated_unprotected_ratio (REAL type)
#             cursor.execute('''
#                 CREATE TABLE IF NOT EXISTS selection_log (
#                     log_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
#                     status TEXT NOT NULL, message TEXT, selected_count INTEGER NOT NULL,
#                     total_in_pool INTEGER NOT NULL, /* Represents total input records processed */
#                     frame_used REAL NOT NULL, /* Stores estimated_unprotected_ratio used */
#                     log_timestamp DATETIME NOT NULL
#                 )
#             ''')
#             # Selected applications table: 'sampling_frame_flag' column removed
#             # 'policy_id' is the name of the column in the DB, which corresponds to 'id' from input.
#             cursor.execute('''
#                 CREATE TABLE IF NOT EXISTS selected_applications (
#                     selected_app_id INTEGER PRIMARY KEY AUTOINCREMENT, selection_log_id INTEGER NOT NULL,
#                     policy_id TEXT NOT NULL UNIQUE, /* Maps to 'id' from input */
#                     advisor_id TEXT,
#                     application_receive_date TEXT,
#                     FOREIGN KEY (selection_log_id) REFERENCES selection_log (log_id)
#                 )
#             ''')
#             try:
#                 cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_id ON selected_applications (policy_id)')
#                 logging.info("Ensured UNIQUE index exists on selected_applications.policy_id")
#             except sqlite3.Error as e: logging.warning(f"Could not create UNIQUE index: {e}")
#             conn.commit()
#             logging.info(f"Database setup complete or tables already exist: {db_path}")
#     except sqlite3.Error as e: logging.error(f"Database setup error: {e}"); raise

# def get_previously_selected_policies(db_path):
#     """Queries the database and returns a set of all previously selected policy IDs."""
#     previously_selected = set()
#     try:
#         if not os.path.exists(db_path):
#              logging.warning(f"DB file not found at {db_path} during get_previously_selected. Returning empty set.")
#              return previously_selected
#         with sqlite3.connect(db_path) as conn:
#             cursor = conn.cursor()
#             cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='selected_applications'")
#             if cursor.fetchone():
#                 cursor.execute("SELECT policy_id FROM selected_applications") # policy_id is correct here for DB
#                 previously_selected = {row[0] for row in cursor.fetchall()}
#                 logging.info(f"Found {len(previously_selected)} previously selected policy IDs in DB.")
#             else:
#                 logging.warning("'selected_applications' table not found. Assuming no previous selections.")
#     except sqlite3.Error as e: logging.error(f"DB error fetching previous policies: {e}")
#     return previously_selected

# def log_attempt(db_path, batch_id, status, message, selected_count, total_input_records, estimated_unprotected_ratio_used):
#     """Logs the result of a selection attempt to the database."""
#     log_timestamp = datetime.now(); log_id = None
#     try:
#         with sqlite3.connect(db_path) as conn:
#             cursor = conn.cursor()
#             cursor.execute('''
#                 INSERT INTO selection_log
#                 (batch_id, status, message, selected_count, total_in_pool, frame_used, log_timestamp)
#                 VALUES (?, ?, ?, ?, ?, ?, ?)
#             ''', (batch_id, status, message, selected_count, total_input_records, estimated_unprotected_ratio_used, log_timestamp))
#             log_id = cursor.lastrowid; conn.commit()
#             logging.info(f"Logged attempt: Batch={batch_id}, Status={status}, EstUnprotectedRatio={estimated_unprotected_ratio_used:.4f}, LogID={log_id}")
#             return log_id
#     except sqlite3.Error as e: logging.error(f"Failed to log attempt for Batch={batch_id}: {e}"); return None

# def save_selected_apps(db_path, selected_records_df, log_id): # Parameter is a DataFrame
#     """Saves the DataFrame of selected application records to the database."""
#     if selected_records_df is None or selected_records_df.empty or log_id is None:
#         logging.info("No applications selected (DataFrame is empty) or log ID missing, skipping save.")
#         return True
    
#     df_to_save = selected_records_df.copy()
    
#     # Rename input 'id' column to 'policy_id' for database storage
#     if 'id' in df_to_save.columns:
#         df_to_save.rename(columns={'id': 'policy_id'}, inplace=True)
#     else:
#         # This should not happen if input validation passed, but as a safeguard
#         logging.error("'id' column missing from selected records DataFrame. Cannot map to 'policy_id'.")
#         raise ValueError("Selected records DataFrame missing 'id' column.")

#     df_to_save['selection_log_id'] = log_id

#     # Ensure only the columns defined for the DB table are present
#     cols_to_save_in_db = [col for col in SELECTED_APP_DB_COLUMNS if col in df_to_save.columns]
    
#     # Check if all essential DB columns (especially policy_id after rename) are in the df_to_save
#     missing_db_cols = [col for col in SELECTED_APP_DB_COLUMNS if col not in df_to_save.columns]
#     if missing_db_cols:
#         logging.error(f"After preparing for DB, missing essential columns: {missing_db_cols}. Aborting save.")
#         raise ValueError(f"Cannot save selected apps, missing DB columns: {missing_db_cols}")

#     df_to_save = df_to_save[cols_to_save_in_db] # Select only defined DB columns

#     try:
#         with sqlite3.connect(db_path) as conn:
#             df_to_save.to_sql('selected_applications', conn, if_exists='append', index=False)
#             conn.commit()
#             logging.info(f"Successfully saved {len(df_to_save)} selected applications for Log ID {log_id}.")
#             return True
#     except sqlite3.IntegrityError as e: logging.error(f"Integrity error saving selected apps (duplicate policy_id?): {e}"); raise
#     except sqlite3.Error as e: logging.error(f"Failed to save selected applications for Log ID {log_id}: {e}"); raise
#     except Exception as e: logging.error(f"An unexpected error occurred during saving selected applications for Log ID {log_id}: {e}"); raise


# # --- Main Selection Logic Function (New Per-Policy Logic) ---

# def run_selection(input_df, db_path, batch_id, target_overall_rate, estimated_unprotected_ratio):
#     """
#     Performs per-policy probabilistic selection based on a derived probability
#     for unprotected items, to achieve an overall target rate.
#     Excludes protected items and previously selected items.
#     """
#     status = 'ERROR'; message = ''; log_id = None
#     selected_policies_for_this_batch_rows = [] # List to store rows (as dicts) of selected policies

#     # Counters for logging
#     num_input_records = 0
#     num_protected_skipped = 0
#     num_previously_selected_skipped = 0
#     num_passed_probability_gate = 0
#     num_selected_for_this_batch = 0

#     try:
#         setup_database(db_path)
#         previously_selected_ids_from_db = get_previously_selected_policies(db_path)

#         logging.info(f"--- Starting selection for Batch ID: {batch_id} (New Per-Policy Derived Rate Logic) ---")
#         if not isinstance(input_df, pd.DataFrame) or input_df.empty:
#              raise ValueError("Input data must be a non-empty pandas DataFrame.")
#         num_input_records = len(input_df)
#         logging.info(f"Processing {num_input_records} total records from input DataFrame.")
#         logging.info(f"Target overall selection rate: {target_overall_rate*100:.2f}%")
#         logging.info(f"Estimated unprotected ratio: {estimated_unprotected_ratio*100:.2f}%")

#         if not (0 < estimated_unprotected_ratio <= 1): # Check ratio is valid
#             raise ValueError("Estimated unprotected ratio must be > 0 and <= 1.")
        
#         derived_probability_for_unprotected = target_overall_rate / estimated_unprotected_ratio
#         logging.info(f"Derived probability for unprotected, non-historical items: {derived_probability_for_unprotected*100:.4f}%")

#         if derived_probability_for_unprotected > 1.0:
#             logging.warning(f"Derived probability ({derived_probability_for_unprotected*100:.2f}%) is > 100%. Capping at 100%.")
#             derived_probability_for_unprotected = 1.0
#         elif derived_probability_for_unprotected < 0: # Check for negative if target_overall_rate was negative
#              logging.warning(f"Derived probability ({derived_probability_for_unprotected*100:.2f}%) is < 0%. Setting to 0%.")
#              derived_probability_for_unprotected = 0.0


#         all_apps_df = input_df.copy()

#         # --- Column Existence and Type Validation ---
#         missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in all_apps_df.columns]
#         if missing_cols: raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
        
#         # Convert relevant input columns to string and strip whitespace
#         for col in ['id', 'protected_class', 'application_receive_date', 'advisor_id', 'branch_name']:
#              all_apps_df[col] = all_apps_df[col].astype(str).str.strip()
#         # xml_blob can remain as is, or convert to str if there's a chance of non-string types
#         if 'xml_blob' in all_apps_df.columns:
#             all_apps_df['xml_blob'] = all_apps_df['xml_blob'].astype(str)


#         logging.info("Performing detailed data validation...")
#         validation_errors = []
#         # id: check non-empty
#         if all_apps_df['id'].eq('').any(): validation_errors.append("Empty 'id' found.")
#         # id: check for duplicates within input
#         if all_apps_df['id'].duplicated().any():
#              duplicates = all_apps_df[all_apps_df['id'].duplicated()]['id'].unique()
#              validation_errors.append(f"Duplicate id(s) in input: {', '.join(duplicates)}")
#         # protected_class: convert to numeric 0 or 1
#         try:
#             flag_map = {'true': 1, '1': 1, 'yes': 1, 't': 1, 'false': 0, '0': 0, 'no': 0, 'f': 0, '': 0}
#             all_apps_df['protected_class_numeric'] = all_apps_df['protected_class'].str.lower().map(flag_map).fillna(0).astype(int)
#             if (~all_apps_df['protected_class_numeric'].isin([0, 1])).any(): validation_errors.append("Invalid 'protected_class' values (must be 0 or 1 after conversion).")
#         except Exception as e: validation_errors.append(f"Error processing 'protected_class': {e}")
#         # application_receive_date: validate format and range
#         try:
#             all_apps_df['app_receive_dt_obj'] = pd.to_datetime(all_apps_df['application_receive_date'], errors='coerce')
#             invalid_dates = all_apps_df['app_receive_dt_obj'].isnull() & all_apps_df['application_receive_date'].ne('')
#             if invalid_dates.any(): validation_errors.append(f"Unparseable 'application_receive_date': {invalid_dates.sum()} rows.")
#             today = pd.Timestamp.now().normalize(); min_allowed_date = pd.Timestamp('2000-01-01')
#             valid_dates_mask = all_apps_df['app_receive_dt_obj'].notna()
#             if valid_dates_mask.any():
#                 if (all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'] > today).any(): validation_errors.append("Future 'application_receive_date' found.")
#                 if (all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'] < min_allowed_date).any(): validation_errors.append(f"'application_receive_date' before {min_allowed_date.date()} found.")
#             all_apps_df.loc[valid_dates_mask, 'application_receive_date'] = all_apps_df.loc[valid_dates_mask, 'app_receive_dt_obj'].dt.strftime('%Y-%m-%d')
#             all_apps_df.loc[all_apps_df['app_receive_dt_obj'].isnull(), 'application_receive_date'] = '' # Ensure unparseable become empty string
#         except Exception as e: validation_errors.append(f"Error processing 'application_receive_date': {e}")
#         finally:
#              if 'app_receive_dt_obj' in all_apps_df.columns: all_apps_df = all_apps_df.drop(columns=['app_receive_dt_obj'])
        
#         if validation_errors: raise ValueError("Input data validation failed:\n- " + "\n- ".join(validation_errors))
#         logging.info("Input data validation successful.")
#         # --- End Validation ---

#         logging.info("Applying per-policy selection with derived probability...")
#         for index, policy_row in all_apps_df.iterrows():
#             current_id = policy_row['id']
#             # Use the validated numeric protected_class column
#             is_protected = policy_row['protected_class_numeric'] == 1

#             # 1. Check Protected Class
#             if is_protected:
#                 num_protected_skipped += 1
#                 continue

#             # 2. Check Non-Reselection (Historical)
#             if current_id in previously_selected_ids_from_db:
#                 num_previously_selected_skipped += 1
#                 continue
            
#             # 3. Apply Derived Probability (only if > 0)
#             if derived_probability_for_unprotected > 0 and random.random() < derived_probability_for_unprotected:
#                 num_passed_probability_gate += 1
                
#                 # SELECT THE POLICY - store the original row data (as dict)
#                 selected_policies_for_this_batch_rows.append(policy_row.to_dict())
#                 num_selected_for_this_batch += 1
#                 # Add to DB set for this run to ensure it's caught by next run's get_previously_selected_policies
#                 previously_selected_ids_from_db.add(current_id) 
            
#         # Create final DataFrame from the list of selected record dictionaries
#         final_selected_df = pd.DataFrame(selected_policies_for_this_batch_rows)
#         # If selected_df is not empty, ensure it has the columns expected by save_selected_apps
#         # (which are the input columns, 'id' will be renamed to 'policy_id' in save_selected_apps)
#         if not final_selected_df.empty:
#             # Select only the columns that were in the original input and are required
#             # This assumes policy_row.to_dict() includes all original columns.
#             cols_from_input = [col for col in REQUIRED_INPUT_COLUMNS if col in final_selected_df.columns]
#             final_selected_df = final_selected_df[cols_from_input]


#         status = 'SUCCESS'
#         message = (f"Processed {num_input_records} records. "
#                    f"Selected for this batch: {num_selected_for_this_batch}. "
#                    f"Skipped (protected): {num_protected_skipped}. "
#                    f"Skipped (already in DB): {num_previously_selected_skipped}. "
#                    f"Passed derived probability gate (and selected): {num_passed_probability_gate}.") # Corrected variable name
#         logging.info(message)

#     except ValueError as e: message = str(e); logging.error(message); status = 'ERROR'
#     except TypeError as e: message = str(e); logging.error(message); status = 'ERROR'
#     except Exception as e: message = f"An unexpected error occurred: {e}"; logging.exception(message); status = 'ERROR'
#     finally:
#         # frame_used now stores the estimated_unprotected_ratio
#         log_id = log_attempt(db_path, batch_id, status, message, num_selected_for_this_batch, num_input_records, estimated_unprotected_ratio) # Pass ratio
#         save_successful = False
#         if status == 'SUCCESS' and num_selected_for_this_batch > 0 and log_id is not None:
#             try:
#                 save_successful = save_selected_apps(db_path, final_selected_df, log_id)
#             except sqlite3.IntegrityError as ie: status = 'ERROR'; message = f"IntegrityError: Failed to save. Selected: {num_selected_for_this_batch}"; logging.error(f"DB INTEGRITY ERROR: Batch={batch_id}, LogID={log_id}: {ie}")
#             except Exception as e: status = 'ERROR'; message = f"Failed to save apps: {e}"; logging.error(f"CRITICAL: Failed to save for LogID={log_id}: {e}")
#             if status == 'ERROR' and log_id is not None: # If save failed, update the log entry
#                  try:
#                     with sqlite3.connect(db_path) as conn: conn.execute("UPDATE selection_log SET status = ?, message = ? WHERE log_id = ?", (status, message, log_id)); conn.commit(); logging.info(f"Updated log {log_id} status to ERROR due to save failure.")
#                  except Exception as update_e: logging.error(f"Failed to update log status for {log_id}: {update_e}")
#         elif status == 'ERROR':
#              logging.warning(f"Batch {batch_id} finished with ERROR. No apps saved.")
#     return log_id is not None


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
# Define required columns for the INPUT data based on new requirements
REQUIRED_INPUT_COLUMNS = [
    'id', 'protected_class', 'xml_blob', # Core for new logic
    'application_receive_date', 'advisor_id', 'branch_name' # Contextual, for storage
]
# Define columns for the selected_applications DATABASE table
# 'policy_id' in DB maps to 'id' from input.
# 'sampling_frame_flag' is no longer stored for selected items as they are, by definition, not protected.
SELECTED_APP_DB_COLUMNS = [
    'selection_log_id', 'policy_id', 'advisor_id',
    'application_receive_date'
]

# --- Database Functions ---

def setup_database(db_path):
    """Creates the necessary SQLite tables if they don't exist."""
    try:
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir); logging.info(f"Created directory for database: {db_dir}")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Log table: frame_used now stores estimated_unprotected_ratio (REAL type)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selection_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
                    status TEXT NOT NULL, message TEXT, selected_count INTEGER NOT NULL,
                    total_in_pool INTEGER NOT NULL, /* Represents total input records processed */
                    frame_used REAL NOT NULL, /* Stores estimated_unprotected_ratio used */
                    log_timestamp DATETIME NOT NULL
                )
            ''')
            # Selected applications table: 'sampling_frame_flag' column removed
            # 'policy_id' is the name of the column in the DB, which corresponds to 'id' from input.
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selected_applications (
                    selected_app_id INTEGER PRIMARY KEY AUTOINCREMENT, selection_log_id INTEGER NOT NULL,
                    policy_id TEXT NOT NULL UNIQUE, /* Maps to 'id' from input */
                    advisor_id TEXT,
                    application_receive_date TEXT,
                    FOREIGN KEY (selection_log_id) REFERENCES selection_log (log_id)
                )
            ''')
            try:
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_id ON selected_applications (policy_id)')
                logging.info("Ensured UNIQUE index exists on selected_applications.policy_id")
            except sqlite3.Error as e: logging.warning(f"Could not create UNIQUE index: {e}")
            conn.commit()
            logging.info(f"Database setup complete or tables already exist: {db_path}")
    except sqlite3.Error as e: logging.error(f"Database setup error: {e}"); raise

def get_previously_selected_policies(db_path):
    """Queries the database and returns a set of all previously selected policy IDs."""
    previously_selected = set()
    try:
        if not os.path.exists(db_path):
             logging.warning(f"DB file not found at {db_path} during get_previously_selected. Returning empty set.")
             return previously_selected
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='selected_applications'")
            if cursor.fetchone():
                cursor.execute("SELECT policy_id FROM selected_applications") # policy_id is correct here for DB
                previously_selected = {row[0] for row in cursor.fetchall()}
                logging.info(f"Found {len(previously_selected)} previously selected policy IDs in DB.")
            else:
                logging.warning("'selected_applications' table not found. Assuming no previous selections.")
    except sqlite3.Error as e: logging.error(f"DB error fetching previous policies: {e}")
    return previously_selected

def log_attempt(db_path, batch_id, status, message, selected_count, total_input_records, estimated_unprotected_ratio_used):
    """Logs the result of a selection attempt to the database."""
    log_timestamp = datetime.now(); log_id = None
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO selection_log
                (batch_id, status, message, selected_count, total_in_pool, frame_used, log_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (batch_id, status, message, selected_count, total_input_records, estimated_unprotected_ratio_used, log_timestamp))
            log_id = cursor.lastrowid; conn.commit()
            logging.info(f"Logged attempt: Batch={batch_id}, Status={status}, EstUnprotectedRatio={estimated_unprotected_ratio_used:.4f}, LogID={log_id}")
            return log_id
    except sqlite3.Error as e: logging.error(f"Failed to log attempt for Batch={batch_id}: {e}"); return None

def save_selected_apps(db_path, selected_records_df, log_id): # Parameter is a DataFrame
    """Saves the DataFrame of selected application records to the database."""
    if selected_records_df is None or selected_records_df.empty or log_id is None:
        logging.info("No applications selected (DataFrame is empty) or log ID missing, skipping save.")
        return True
    
    df_to_save = selected_records_df.copy()
    
    # Rename input 'id' column to 'policy_id' for database storage
    if 'id' in df_to_save.columns:
        df_to_save.rename(columns={'id': 'policy_id'}, inplace=True)
    else:
        # This should not happen if input validation passed, but as a safeguard
        logging.error("'id' column missing from selected records DataFrame. Cannot map to 'policy_id'.")
        raise ValueError("Selected records DataFrame missing 'id' column.")

    df_to_save['selection_log_id'] = log_id

    # Ensure only the columns defined for the DB table are present
    cols_to_save_in_db = [col for col in SELECTED_APP_DB_COLUMNS if col in df_to_save.columns]
    
    # Check if all essential DB columns (especially policy_id after rename) are in the df_to_save
    missing_db_cols = [col for col in SELECTED_APP_DB_COLUMNS if col not in df_to_save.columns]
    if missing_db_cols:
        logging.error(f"After preparing for DB, missing essential columns: {missing_db_cols}. Aborting save.")
        raise ValueError(f"Cannot save selected apps, missing DB columns: {missing_db_cols}")

    df_to_save = df_to_save[cols_to_save_in_db] # Select only defined DB columns

    try:
        with sqlite3.connect(db_path) as conn:
            df_to_save.to_sql('selected_applications', conn, if_exists='append', index=False)
            conn.commit()
            logging.info(f"Successfully saved {len(df_to_save)} selected applications for Log ID {log_id}.")
            return True
    except sqlite3.IntegrityError as e: logging.error(f"Integrity error saving selected apps (duplicate policy_id?): {e}"); raise
    except sqlite3.Error as e: logging.error(f"Failed to save selected applications for Log ID {log_id}: {e}"); raise
    except Exception as e: logging.error(f"An unexpected error occurred during saving selected applications for Log ID {log_id}: {e}"); raise


# --- Main Selection Logic Function (New Per-Policy Logic) ---

def run_selection(input_df, db_path, batch_id, target_overall_rate, estimated_unprotected_ratio):
    """
    Performs per-policy probabilistic selection based on a derived probability
    for unprotected items, to achieve an overall target rate.
    Excludes protected items and previously selected items.
    """
    status = 'ERROR'; message = ''; log_id = None
    selected_policies_for_this_batch_rows = [] # List to store rows (as dicts) of selected policies

    # Counters for logging
    num_input_records = 0
    num_protected_skipped = 0
    num_previously_selected_skipped = 0
    num_passed_probability_gate = 0
    num_selected_for_this_batch = 0

    try:
        setup_database(db_path)
        previously_selected_ids_from_db = get_previously_selected_policies(db_path)

        logging.info(f"--- Starting selection for Batch ID: {batch_id} (New Per-Policy Derived Rate Logic) ---")
        if not isinstance(input_df, pd.DataFrame) or input_df.empty:
             raise ValueError("Input data must be a non-empty pandas DataFrame.")
        num_input_records = len(input_df)
        logging.info(f"Processing {num_input_records} total records from input DataFrame.")
        logging.info(f"Target overall selection rate: {target_overall_rate*100:.2f}%")
        logging.info(f"Estimated unprotected ratio: {estimated_unprotected_ratio*100:.2f}%")

        if not (0 < estimated_unprotected_ratio <= 1): # Check ratio is valid
            raise ValueError("Estimated unprotected ratio must be > 0 and <= 1.")
        
        derived_probability_for_unprotected = target_overall_rate / estimated_unprotected_ratio
        logging.info(f"Derived probability for unprotected, non-historical items: {derived_probability_for_unprotected*100:.4f}%")

        if derived_probability_for_unprotected > 1.0:
            logging.warning(f"Derived probability ({derived_probability_for_unprotected*100:.2f}%) is > 100%. Capping at 100%.")
            derived_probability_for_unprotected = 1.0
        elif derived_probability_for_unprotected < 0: # Check for negative if target_overall_rate was negative
             logging.warning(f"Derived probability ({derived_probability_for_unprotected*100:.2f}%) is < 0%. Setting to 0%.")
             derived_probability_for_unprotected = 0.0


        all_apps_df = input_df.copy()

        # --- Column Existence and Type Validation ---
        missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in all_apps_df.columns]
        if missing_cols: raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
        
        # Convert relevant input columns to string and strip whitespace
        for col in ['id', 'protected_class', 'application_receive_date', 'advisor_id', 'branch_name']:
             all_apps_df[col] = all_apps_df[col].astype(str).str.strip()
        # xml_blob can remain as is, or convert to str if there's a chance of non-string types
        if 'xml_blob' in all_apps_df.columns:
            all_apps_df['xml_blob'] = all_apps_df['xml_blob'].astype(str)


        logging.info("Performing detailed data validation...")
        validation_errors = []
        # id: check non-empty
        if all_apps_df['id'].eq('').any(): validation_errors.append("Empty 'id' found.")
        # id: check for duplicates within input
        if all_apps_df['id'].duplicated().any():
             duplicates = all_apps_df[all_apps_df['id'].duplicated()]['id'].unique()
             validation_errors.append(f"Duplicate id(s) in input: {', '.join(duplicates)}")
        # protected_class: convert to numeric 0 or 1
        try:
            flag_map = {'true': 1, '1': 1, 'yes': 1, 't': 1, 'false': 0, '0': 0, 'no': 0, 'f': 0, '': 0}
            all_apps_df['protected_class_numeric'] = all_apps_df['protected_class'].str.lower().map(flag_map).fillna(0).astype(int)
            if (~all_apps_df['protected_class_numeric'].isin([0, 1])).any(): validation_errors.append("Invalid 'protected_class' values (must be 0 or 1 after conversion).")
        except Exception as e: validation_errors.append(f"Error processing 'protected_class': {e}")
        # application_receive_date: validate format and range
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
            all_apps_df.loc[all_apps_df['app_receive_dt_obj'].isnull(), 'application_receive_date'] = '' # Ensure unparseable become empty string
        except Exception as e: validation_errors.append(f"Error processing 'application_receive_date': {e}")
        finally:
             if 'app_receive_dt_obj' in all_apps_df.columns: all_apps_df = all_apps_df.drop(columns=['app_receive_dt_obj'])
        
        if validation_errors: raise ValueError("Input data validation failed:\n- " + "\n- ".join(validation_errors))
        logging.info("Input data validation successful.")
        # --- End Validation ---

        logging.info("Applying per-policy selection with derived probability...")
        for index, policy_row in all_apps_df.iterrows():
            current_id = policy_row['id']
            # Use the validated numeric protected_class column
            is_protected = policy_row['protected_class_numeric'] == 1

            # 1. Check Protected Class
            if is_protected:
                num_protected_skipped += 1
                continue

            # 2. Check Non-Reselection (Historical)
            if current_id in previously_selected_ids_from_db:
                num_previously_selected_skipped += 1
                continue
            
            # 3. Apply Derived Probability (only if > 0)
            if derived_probability_for_unprotected > 0 and random.random() < derived_probability_for_unprotected:
                num_passed_probability_gate += 1
                
                # SELECT THE POLICY - store the original row data (as dict)
                selected_policies_for_this_batch_rows.append(policy_row.to_dict())
                num_selected_for_this_batch += 1
                # Add to DB set for this run to ensure it's caught by next run's get_previously_selected_policies
                previously_selected_ids_from_db.add(current_id) 
            
        # Create final DataFrame from the list of selected record dictionaries
        final_selected_df = pd.DataFrame(selected_policies_for_this_batch_rows)
        # If selected_df is not empty, ensure it has the columns expected by save_selected_apps
        # (which are the input columns, 'id' will be renamed to 'policy_id' in save_selected_apps)
        if not final_selected_df.empty:
            # Select only the columns that were in the original input and are required
            # This assumes policy_row.to_dict() includes all original columns.
            cols_from_input = [col for col in REQUIRED_INPUT_COLUMNS if col in final_selected_df.columns]
            final_selected_df = final_selected_df[cols_from_input]


        status = 'SUCCESS'
        message = (f"Processed {num_input_records} records. "
                   f"Selected for this batch: {num_selected_for_this_batch}. "
                   f"Skipped (protected): {num_protected_skipped}. "
                   f"Skipped (already in DB): {num_previously_selected_skipped}. "
                   f"Passed derived probability gate (and selected): {num_passed_probability_gate}.") # Corrected variable name
        logging.info(message)

    except ValueError as e: message = str(e); logging.error(message); status = 'ERROR'
    except TypeError as e: message = str(e); logging.error(message); status = 'ERROR'
    except Exception as e: message = f"An unexpected error occurred: {e}"; logging.exception(message); status = 'ERROR'
    finally:
        # frame_used now stores the estimated_unprotected_ratio
        log_id = log_attempt(db_path, batch_id, status, message, num_selected_for_this_batch, num_input_records, estimated_unprotected_ratio) # Pass ratio
        save_successful = False
        if status == 'SUCCESS' and num_selected_for_this_batch > 0 and log_id is not None:
            try:
                save_successful = save_selected_apps(db_path, final_selected_df, log_id)
            except sqlite3.IntegrityError as ie: status = 'ERROR'; message = f"IntegrityError: Failed to save. Selected: {num_selected_for_this_batch}"; logging.error(f"DB INTEGRITY ERROR: Batch={batch_id}, LogID={log_id}: {ie}")
            except Exception as e: status = 'ERROR'; message = f"Failed to save apps: {e}"; logging.error(f"CRITICAL: Failed to save for LogID={log_id}: {e}")
            if status == 'ERROR' and log_id is not None: # If save failed, update the log entry
                 try:
                    with sqlite3.connect(db_path) as conn: conn.execute("UPDATE selection_log SET status = ?, message = ? WHERE log_id = ?", (status, message, log_id)); conn.commit(); logging.info(f"Updated log {log_id} status to ERROR due to save failure.")
                 except Exception as update_e: logging.error(f"Failed to update log status for {log_id}: {update_e}")
        elif status == 'ERROR':
             logging.warning(f"Batch {batch_id} finished with ERROR. No apps saved.")
    return log_id is not None
