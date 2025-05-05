import pandas as pd
import random
from datetime import date, timedelta
import uuid
import os

# --- Configuration ---
NUM_RECORDS = 5000
START_DATE_STR = "2025-01-01"
END_DATE_STR = "2025-05-31"
OUTPUT_FILENAME = "mock_input_data_5k.csv"

# Define required columns matching the sampler's expectation
REQUIRED_COLUMNS = [
    'policy_id', 'application_receive_date', 'advisor_id',
    'branch_name', 'sampling_frame_flag'
]

# --- Helper Data ---
ADVISORS = [f'ADV{str(i).zfill(3)}' for i in range(1, 76)] # 75 unique advisors
BRANCHES = ['Maple Branch', 'Oak Office', 'Pine Plaza', 'Cedar Centre', 'Birch Bureau', 'Willow Way']

# --- Generation Function ---

def generate_input_csv(filename, num_records, start_date_str, end_date_str):
    """Generates mock application data and saves it to a CSV file."""

    print(f"Generating {num_records} records for date range {start_date_str} to {end_date_str}...")

    data = []
    try:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)
        total_days = (end_date - start_date).days
        if total_days < 0:
            print("Error: Start date must be before end date.")
            return
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        return

    # Generate records
    for i in range(num_records):
        # Generate random date within the range
        random_number_of_days = random.randrange(total_days + 1) # Include end date
        app_date = start_date + timedelta(days=random_number_of_days)

        # Generate other fields
        policy_id = str(uuid.uuid4()) # Unique ID for each policy
        advisor_id = random.choice(ADVISORS)
        branch_name = random.choice(BRANCHES)

        # Determine sampling frame flag based on month
        app_month = app_date.month
        if app_month <= 2: # January or February
            sampling_frame_flag = 0
        else: # March, April, or May
            # Assign randomly (e.g., 80% chance of being 1)
            sampling_frame_flag = 1 if random.random() < 0.8 else 0

        data.append({
            'policy_id': policy_id,
            'application_receive_date': app_date.strftime('%Y-%m-%d'), # Format as string
            'advisor_id': advisor_id,
            'branch_name': branch_name,
            'sampling_frame_flag': sampling_frame_flag
        })

        # Optional: Print progress indicator
        if (i + 1) % 500 == 0:
            print(f"Generated {i + 1}/{num_records} records...")

    # Create DataFrame
    df = pd.DataFrame(data)

    # Ensure correct column order
    df = df[REQUIRED_COLUMNS]

    # Save to CSV
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nSuccessfully generated and saved {len(df)} records to '{filename}'")
        # Display flag distribution summary
        print("\n--- Flag Distribution Summary ---")
        print(df.groupby(pd.to_datetime(df['application_receive_date']).dt.strftime('%Y-%m'))['sampling_frame_flag'].value_counts(normalize=True).unstack(fill_value=0))

    except Exception as e:
        print(f"\nError saving data to CSV: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    generate_input_csv(OUTPUT_FILENAME, NUM_RECORDS, START_DATE_STR, END_DATE_STR)