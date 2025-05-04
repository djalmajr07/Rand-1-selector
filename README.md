# Application Random Sampler POC

This project implements a Proof-of-Concept (POC) for randomly selecting a percentage of applications based on defined criteria, storing the results, and providing an API to access the selections.

## Features

* Selects applications randomly based on a 1% target rate (minimum 1).
* Auto-detects if flagged applications exist and adjusts the sampling pool accordingly (samples from flagged pool if flags exist, otherwise samples from total pool).
* Prevents re-selection of the same `policy_id` across different batch runs.
* Stores selection logs and details of selected applications in an SQLite database.
* Provides a Flask API with Swagger UI to view logs and retrieve selected applications (JSON or Excel format).
* Reads configuration (database path) from `config.ini`.
* Includes enhanced input data validation.

## Project Structure


.

├── sampler_logic.py     # Core functions for selection, DB interaction, validation

├── main.py              # Main script to run selection process from CSV input

├── api.py               # Flask API server to access results

├── config.ini           # Configuration file (e.g., database path)

├── requirements.txt     # Python package dependencies

└── venv/                # Virtual environment (optional but recommended)

└── input_apps.csv       # Example input CSV file (you need to create this)

└── sampler_data.db      # SQLite database file (created automatically)

└── README.md            # This file


## Setup

1.  **Clone/Download:** Get the project files.
2.  **Create Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    ```
3.  **Activate Virtual Environment:**
    * Windows: `.\venv\Scripts\activate`
    * macOS/Linux: `source venv/bin/activate`
4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
5.  **Configure:** Edit `config.ini` to set the desired `db_path` if you don't want the default (`sampler_data.db`).
6.  **Prepare Input CSV:** Create an input CSV file (e.g., `input_apps.csv`) with the following required columns:
    * `policy_id` (Unique identifier for the application)
    * `application_receive_date` (Date in YYYY-MM-DD or other parseable format)
    * `advisor_id`
    * `branch_name`
    * `sampling_frame_flag` (Value convertible to 1/True for flagged, 0/False otherwise)

## Usage

### 1. Running the Selection Process

Execute the `main.py` script from your terminal, providing the path to your input CSV and a unique batch ID for the run.

```bash
python main.py --input-csv path/to/your/input_apps.csv --batch-id YOUR_UNIQUE_BATCH_ID

Replace path/to/your/input_apps.csv with the actual path to your CSV file.

Replace YOUR_UNIQUE_BATCH_ID with a meaningful identifier (e.g., MAY2025_RUN1).

The script will read the CSV, perform validation, run the selection logic (checking for previous selections in the DB specified in config.ini), and store results in the database. Check the console output for logs and status.

2. Running the API Server
Start the Flask API server from your terminal:

python api.py

The API will start (usually on http://127.0.0.1:5000/ or http://0.0.0.0:5000/). It reads the db_path from config.ini.

3. Accessing the API
Swagger UI (Recommended): Open your web browser and navigate to http://127.0.0.1:5000/apidocs/. This provides an interactive interface to test the API endpoints.

Direct Access / Testing Tools (e.g., Thunder Client, Postman, curl):

Status: GET http://127.0.0.1:5000/status

View All Logs: GET http://127.0.0.1:5000/api/logs

View Selected (JSON): GET http://127.0.0.1:5000/api/selected/YOUR_UNIQUE_BATCH_ID/json

Download Selected (Excel): GET http://127.0.0.1:5000/api/selected/YOUR_UNIQUE_BATCH_ID/excel (This will trigger a file download)

(Replace YOUR_UNIQUE_BATCH_ID with the actual ID used when running main.py)

Next Steps
