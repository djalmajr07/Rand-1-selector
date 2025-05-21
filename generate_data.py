# import pandas as pd
# import random
# from datetime import date, timedelta
# import uuid
# import os

# # --- Configuration ---
# NUM_RECORDS = 5000
# START_DATE_STR = "2025-01-01"
# END_DATE_STR = "2025-05-31"
# OUTPUT_FILENAME = "mock_input_data_5k.csv"

# # Define required columns matching the sampler's expectation
# REQUIRED_COLUMNS = [
#     'policy_id', 'application_receive_date', 'advisor_id',
#     'branch_name', 'sampling_frame_flag'
# ]

# # --- Helper Data ---
# ADVISORS = [f'ADV{str(i).zfill(3)}' for i in range(1, 76)] # 75 unique advisors
# BRANCHES = ['Maple Branch', 'Oak Office', 'Pine Plaza', 'Cedar Centre', 'Birch Bureau', 'Willow Way']

# # --- Generation Function ---

# def generate_input_csv(filename, num_records, start_date_str, end_date_str):
#     """Generates mock application data and saves it to a CSV file."""

#     print(f"Generating {num_records} records for date range {start_date_str} to {end_date_str}...")

#     data = []
#     try:
#         start_date = date.fromisoformat(start_date_str)
#         end_date = date.fromisoformat(end_date_str)
#         total_days = (end_date - start_date).days
#         if total_days < 0:
#             print("Error: Start date must be before end date.")
#             return
#     except ValueError:
#         print("Error: Invalid date format. Please use YYYY-MM-DD.")
#         return

#     # Generate records
#     for i in range(num_records):
#         # Generate random date within the range
#         random_number_of_days = random.randrange(total_days + 1) # Include end date
#         app_date = start_date + timedelta(days=random_number_of_days)

#         # Generate other fields
#         policy_id = str(uuid.uuid4()) # Unique ID for each policy
#         advisor_id = random.choice(ADVISORS)
#         branch_name = random.choice(BRANCHES)

#         # Determine sampling frame flag based on month
#         app_month = app_date.month
#         if app_month <= 2: # January or February
#             sampling_frame_flag = 0
#         else: # March, April, or May
#             # Assign randomly (e.g., 80% chance of being 1)
#             sampling_frame_flag = 1 if random.random() < 0.8 else 0

#         data.append({
#             'policy_id': policy_id,
#             'application_receive_date': app_date.strftime('%Y-%m-%d'), # Format as string
#             'advisor_id': advisor_id,
#             'branch_name': branch_name,
#             'sampling_frame_flag': sampling_frame_flag
#         })

#         # Optional: Print progress indicator
#         if (i + 1) % 500 == 0:
#             print(f"Generated {i + 1}/{num_records} records...")

#     # Create DataFrame
#     df = pd.DataFrame(data)

#     # Ensure correct column order
#     df = df[REQUIRED_COLUMNS]

#     # Save to CSV
#     try:
#         df.to_csv(filename, index=False, encoding='utf-8')
#         print(f"\nSuccessfully generated and saved {len(df)} records to '{filename}'")
#         # Display flag distribution summary
#         print("\n--- Flag Distribution Summary ---")
#         print(df.groupby(pd.to_datetime(df['application_receive_date']).dt.strftime('%Y-%m'))['sampling_frame_flag'].value_counts(normalize=True).unstack(fill_value=0))

#     except Exception as e:
#         print(f"\nError saving data to CSV: {e}")

# # --- Main Execution ---
# if __name__ == "__main__":
#     generate_input_csv(OUTPUT_FILENAME, NUM_RECORDS, START_DATE_STR, END_DATE_STR)

import pandas as pd
import random
from datetime import date, timedelta, datetime
import uuid
import os

# --- Configuration ---
NUM_RECORDS = 10000 # As per user example
START_DATE_STR = (date.today() - timedelta(days=120)).strftime('%Y-%m-%d') # Approx 4 months ago
END_DATE_STR = date.today().strftime('%Y-%m-%d') # Generate up to today
OUTPUT_FILENAME = "input_applications_10k.csv" # New filename
PROTECTED_RATIO = 0.20 # Approx 20% will be marked as protected

# Define columns for the output CSV
# 'id', 'protected_class', 'xml_blob', 'application_receive_date', 'advisor_id', 'branch_name'
OUTPUT_COLUMNS = [
    'id', 'protected_class', 'xml_blob',
    'application_receive_date', 'advisor_id', 'branch_name'
]

# --- Helper Data ---
ADVISORS = [f'ADV{str(i).zfill(3)}' for i in range(1, 76)]
BRANCHES = ['Maple Branch', 'Oak Office', 'Pine Plaza', 'Cedar Centre', 'Birch Bureau', 'Willow Way']

# --- Generation Function ---

def generate_input_csv(filename, num_records, start_date_str, end_date_str, protected_ratio):
    """Generates mock application data with the new schema and saves it to a CSV file."""

    print(f"Generating {num_records} records for date range {start_date_str} to {end_date_str}...")
    print(f"Approximately {protected_ratio*100:.0f}% will be 'protected_class = 1'.")

    data = []
    try:
        start_date_obj = date.fromisoformat(start_date_str)
        end_date_obj = date.fromisoformat(end_date_str)
        total_days = (end_date_obj - start_date_obj).days
        if total_days < 0:
            print("Error: Start date must be before end date.")
            return
        if total_days == 0:
             print("Warning: Start date and end date are the same. All records will have this date.")
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        return

    for i in range(num_records):
        random_number_of_days = random.randrange(total_days + 1) if total_days >=0 else 0
        app_date = start_date_obj + timedelta(days=random_number_of_days)

        policy_id = str(uuid.uuid4()) # Renamed to 'id' for consistency with new reqs
        advisor_id = random.choice(ADVISORS)
        branch_name = random.choice(BRANCHES)

        # Determine protected_class status
        is_protected = 1 if random.random() < protected_ratio else 0

        # Create a simple placeholder XML blob
        xml_blob_content = f"<application_data><id>{policy_id}</id><submission_timestamp>{datetime.now().isoformat()}</submission_timestamp><details>Mock details for app {i+1}</details></application_data>"

        data.append({
            'id': policy_id,
            'protected_class': is_protected,
            'xml_blob': xml_blob_content,
            'application_receive_date': app_date.strftime('%Y-%m-%d'),
            'advisor_id': advisor_id,
            'branch_name': branch_name
        })

        if (i + 1) % 1000 == 0:
            print(f"Generated {i + 1}/{num_records} records...")

    df = pd.DataFrame(data)
    df = df[OUTPUT_COLUMNS] # Ensure correct column order

    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nSuccessfully generated and saved {len(df)} records to '{filename}'")
        print("\n--- Protected Class Distribution Summary ---")
        print(df['protected_class'].value_counts(normalize=True))
    except Exception as e:
        print(f"\nError saving data to CSV: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    generate_input_csv(OUTPUT_FILENAME, NUM_RECORDS, START_DATE_STR, END_DATE_STR, PROTECTED_RATIO)
