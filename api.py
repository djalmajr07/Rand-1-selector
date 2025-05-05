# api.py
import sqlite3
import pandas as pd
from flask import Flask, jsonify, request, Response, abort
from flasgger import Swagger, swag_from 
import io
import logging
import os
import configparser 
import sys
from datetime import datetime 

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
CONFIG_PATH = 'config.ini'

def get_db_path_from_config(config_path=CONFIG_PATH):
    """Reads the database path from the config file."""
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        return None # Indicate config file is missing
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        # Read db_path from [Database] section, provide a default fallback
        db_path = config.get('Database', 'db_path', fallback='sampler_data.db')
        return db_path
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
         logging.error(f"Configuration error in '{config_path}': {e}")
         return None # Indicate config error
    except Exception as e:
        logging.error(f"Error reading configuration: {e}")
        return None

DB_PATH = get_db_path_from_config()

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Define Schemas Directly for Swagger Config ---
log_entry_schema = {
    "type": "object",
    "properties": {
        "log_id": {"type": "integer", "example": 1},
        "batch_id": {"type": "string", "example": "MAY2025_RUN1"},
        "status": {"type": "string", "example": "SUCCESS"},
        "message": {"type": "string", "example": "Selected 10 total (no flags found) from 1000 eligible (after exclusions)."},
        "selected_count": {"type": "integer", "example": 10},
        "total_in_pool": {"type": "integer", "example": 1000},
        "frame_used": {"type": "integer", "example": 0},
        "log_timestamp": {"type": "string", "format": "date-time", "example": "2025-05-04 14:00:00.123456"}
    }
}

#adjust to EQ details when a better view of the sample is available
selected_app_schema = {
    "type": "object",
    "properties": {
        "policy_id": {"type": "string", "format": "uuid", "example": "a1b2c3d4-e5f6-7890-1234-567890abcdef"},
        "advisor_id": {"type": "string", "example": "ADV001"},
        "sampling_frame_flag": {"type": "integer", "example": 1},
        "application_receive_date": {"type": "string", "format": "date", "example": "2025-04-20"}
    }
}
# Add schema for combined output (including batch_id and timestamp)
selected_app_with_batch_schema = {
    "type": "object",
    "properties": {
        "policy_id": {"type": "string", "format": "uuid", "example": "a1b2c3d4-e5f6-7890-1234-567890abcdef"},
        "advisor_id": {"type": "string", "example": "ADV001"},
        "sampling_frame_flag": {"type": "integer", "example": 1},
        "application_receive_date": {"type": "string", "format": "date", "example": "2025-04-20"},
        "batch_id": {"type": "string", "example": "MAY2025_RUN1"},
        "selection_timestamp": {"type": "string", "format": "date-time", "example": "2025-05-04 14:00:00.123456"}
    }
}


# Configure Flasgger with definitions under components/schemas structure
app.config['SWAGGER'] = {
    'title': 'Application Sampler API',
    'uiversion': 3, # Use Swagger UI 3
    'openapi': '3.0.2',
    'description': 'API for retrieving application selection logs and results.',
    # *** Define schemas under components/schemas for OpenAPI 3 standard ***
    'components': {
        'schemas': {
            'LogEntry': log_entry_schema,
            'SelectedApplication': selected_app_schema,
            'SelectedApplicationWithBatch': selected_app_with_batch_schema # Add new schema
        }
    }
    # Add more Swagger config if needed (e.g., contact info, license)
}
swagger = Swagger(app) # Initialize Flasgger

# --- Helper Function ---
def get_db_connection():
    """Establishes connection to the SQLite database."""
    if DB_PATH is None: logging.error("Database path not configured."); return None
    if not os.path.exists(DB_PATH): logging.error(f"Database file not found: {DB_PATH}"); return None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e: logging.error(f"Database connection error: {e}"); return None

# --- API Endpoints ---

@app.route('/status', methods=['GET'])
@swag_from({
    'summary': 'API Status Check',
    'description': 'Returns the current status of the API and the configured database path.',
    'tags': ['Status'],
    'responses': {
        200: {
            'description': 'API is running.',
            'content': { 'application/json': { 'schema': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string', 'example': 'API is running'},
                    'database_path': {'type': 'string', 'example': 'sampler_data.db'}
                }}}}
        },
        503: {'description': 'Database path not configured or file not found.'}
    }
})
def status():
    """API Status Check Endpoint."""
    if DB_PATH is None or not os.path.exists(DB_PATH):
         abort(503, description=f"Database path not configured or file missing. Path: {DB_PATH}")
    return jsonify({"status": "API is running", "database_path": DB_PATH})

@app.route('/api/logs', methods=['GET'])
@swag_from({
    'summary': 'Get All Selection Logs',
    'description': 'Retrieves all entries from the selection log table, ordered by timestamp descending.',
    'tags': ['Logs'],
    'responses': {
        200: {
            'description': 'A list of log entries.',
            'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/components/schemas/LogEntry'}}}}
        },
        404: {'description': 'Log table not found or empty.'},
        500: {'description': 'Internal server error fetching logs.'},
        503: {'description': 'Database service unavailable.'}
    }
})
def get_logs():
    """Retrieve all logs from the selection_log table."""
    conn = get_db_connection()
    if conn is None: abort(503, description="Database service unavailable.")
    try:
        logs_df = pd.read_sql_query("SELECT * FROM selection_log ORDER BY log_timestamp DESC", conn)
        conn.close()
        logs_df['log_timestamp'] = logs_df['log_timestamp'].astype(str)
        return jsonify(logs_df.to_dict(orient='records'))
    except pd.io.sql.DatabaseError as e:
         logging.warning(f"Could not read logs table: {e}"); conn.close()
         abort(404, description="Log table not found or empty.")
    except Exception as e:
        logging.error(f"Error fetching logs: {e}")
        if conn: conn.close(); abort(500, description="Internal server error fetching logs.")


@app.route('/api/selected/<string:batch_id>/json', methods=['GET'])
@swag_from({
    'summary': 'Get Selected Applications by Batch (JSON)',
    'description': 'Retrieves details of applications selected in a specific batch run.',
    'tags': ['Selections'],
    'parameters': [
        { 'name': 'batch_id', 'in': 'path', 'required': True, 'description': 'The Batch ID to retrieve.', 'schema': {'type': 'string'} }
    ],
    'responses': {
        200: {
            'description': 'A list of selected application details.',
            'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/components/schemas/SelectedApplication'}}}}
        },
        404: {'description': 'Batch ID not found or no applications selected for this batch.'},
        500: {'description': 'Internal server error fetching data.'},
        503: {'description': 'Database service unavailable.'}
    }
})
def get_selected_by_batch_json(batch_id):
    """Retrieve selected applications for a specific batch as JSON."""
    conn = get_db_connection()
    if conn is None: abort(503, description="Database service unavailable.")
    try:
        query = """
            SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
                   sa.application_receive_date
            FROM selected_applications sa
            JOIN selection_log sl ON sa.selection_log_id = sl.log_id
            WHERE sl.batch_id = ? ORDER BY sa.policy_id;
        """
        selected_df = pd.read_sql_query(query, conn, params=(batch_id,))
        conn.close()
        if selected_df.empty:
            abort(404, description=f"No selected applications found for Batch ID: {batch_id}")
        return jsonify(selected_df.to_dict(orient='records'))
    except pd.io.sql.DatabaseError as e:
         logging.warning(f"Could not read selected_applications table: {e}")
         conn.close(); abort(404, description="Selected applications table access error.")
    except Exception as e:
        logging.error(f"Error fetching selected apps for batch {batch_id}: {e}")
        if conn: conn.close(); abort(500, description="Internal server error fetching data.")


@app.route('/api/selected/<string:batch_id>/excel', methods=['GET'])
@swag_from({
    'summary': 'Download Selected Applications by Batch (Excel)',
    'description': 'Downloads an Excel file containing details of applications selected in a specific batch run.',
    'tags': ['Selections'],
    'parameters': [
        { 'name': 'batch_id', 'in': 'path', 'required': True, 'description': 'The Batch ID to retrieve.', 'schema': {'type': 'string'} }
    ],
    'responses': {
        200: {
            'description': 'An Excel file download.',
            'content': {'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {}}
        },
        404: {'description': 'Batch ID not found or no applications selected for this batch.'},
        500: {'description': 'Internal server error generating file.'},
        503: {'description': 'Database service unavailable.'}
    }
})
def get_selected_by_batch_excel(batch_id):
    """Retrieve selected applications for a specific batch as an Excel file."""
    conn = get_db_connection()
    if conn is None: abort(503, description="Database service unavailable.")
    try:
        query = """
            SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
                   sa.application_receive_date, sl.log_timestamp as selection_timestamp
            FROM selected_applications sa
            JOIN selection_log sl ON sa.selection_log_id = sl.log_id
            WHERE sl.batch_id = ? ORDER BY sa.policy_id;
        """
        selected_df = pd.read_sql_query(query, conn, params=(batch_id,))
        conn.close()
        if selected_df.empty:
            abort(404, description=f"No selected applications found for Batch ID: {batch_id}")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            selected_df.to_excel(writer, index=False, sheet_name='Selected Applications')
        output.seek(0)
        filename = f"selected_apps_{batch_id}.xlsx"
        return Response(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment;filename={filename}'}
        )
    except pd.io.sql.DatabaseError as e:
         logging.warning(f"Could not read selected_applications table for Excel: {e}")
         conn.close(); abort(404, description="Selected applications table access error.")
    except Exception as e:
        logging.error(f"Error generating Excel for batch {batch_id}: {e}")
        if conn: conn.close(); abort(500, description="Internal server error generating file.")

# --- *** NEW ENDPOINTS START HERE *** ---

@app.route('/api/selected/all/json', methods=['GET'])
@swag_from({
    'summary': 'Get ALL Selected Applications (JSON)',
    'description': 'Retrieves details of ALL applications selected across ALL batch runs.',
    'tags': ['Selections'],
    'responses': {
        200: {
            'description': 'A list of all selected application details, including batch ID and timestamp.',
            'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/components/schemas/SelectedApplicationWithBatch'}}}}
        },
        404: {'description': 'Selected applications table not found or empty.'},
        500: {'description': 'Internal server error fetching data.'},
        503: {'description': 'Database service unavailable.'}
    }
})
def get_all_selected_json():
    """Retrieve ALL selected applications as JSON."""
    conn = get_db_connection()
    if conn is None: abort(503, description="Database service unavailable.")
    try:
        # Query includes batch_id and selection_timestamp from the log table
        query = """
            SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
                   sa.application_receive_date, sl.batch_id, sl.log_timestamp as selection_timestamp
            FROM selected_applications sa
            JOIN selection_log sl ON sa.selection_log_id = sl.log_id
            ORDER BY sl.log_timestamp DESC, sa.policy_id;
        """
        selected_df = pd.read_sql_query(query, conn)
        conn.close()
        if selected_df.empty:
            abort(404, description="No selected applications found in the database.")
        # Ensure timestamp is string for JSON
        selected_df['selection_timestamp'] = selected_df['selection_timestamp'].astype(str)
        return jsonify(selected_df.to_dict(orient='records'))
    except pd.io.sql.DatabaseError as e:
         logging.warning(f"Could not read selected_applications table: {e}")
         conn.close(); abort(404, description="Selected applications table access error.")
    except Exception as e:
        logging.error(f"Error fetching all selected apps: {e}")
        if conn: conn.close(); abort(500, description="Internal server error fetching data.")


@app.route('/api/selected/all/excel', methods=['GET'])
@swag_from({
    'summary': 'Download ALL Selected Applications (Excel)',
    'description': 'Downloads an Excel file containing details of ALL applications selected across ALL batch runs.',
    'tags': ['Selections'],
    'responses': {
        200: {
            'description': 'An Excel file download.',
            'content': {'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {}}
        },
        404: {'description': 'Selected applications table not found or empty.'},
        500: {'description': 'Internal server error generating file.'},
        503: {'description': 'Database service unavailable.'}
    }
})
def get_all_selected_excel():
    """Retrieve ALL selected applications as an Excel file."""
    conn = get_db_connection()
    if conn is None: abort(503, description="Database service unavailable.")
    try:
        # Query includes batch_id and selection_timestamp
        query = """
            SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
                   sa.application_receive_date, sl.batch_id, sl.log_timestamp as selection_timestamp
            FROM selected_applications sa
            JOIN selection_log sl ON sa.selection_log_id = sl.log_id
            ORDER BY sl.log_timestamp DESC, sa.policy_id;
        """
        selected_df = pd.read_sql_query(query, conn)
        conn.close()
        if selected_df.empty:
            abort(404, description="No selected applications found in the database.")

        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            selected_df.to_excel(writer, index=False, sheet_name='All Selected Applications')
        output.seek(0)
        # Prepare filename and response headers for download
        # *** Use datetime object correctly ***
        filename = f"all_selected_apps_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return Response(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment;filename={filename}'}
        )
    except pd.io.sql.DatabaseError as e:
         logging.warning(f"Could not read selected_applications table for Excel: {e}")
         conn.close(); abort(404, description="Selected applications table access error.")
    except Exception as e:
        logging.error(f"Error generating Excel for all selected apps: {e}")
        if conn: conn.close(); abort(500, description="Internal server error generating file.")

# --- *** END NEW ENDPOINTS *** ---


# --- Run the Flask App ---
if __name__ == '__main__':
    # Check configuration and DB existence before starting
    if DB_PATH is None:
         print("ERROR: Database path could not be determined from config file. API cannot start.")
         sys.exit(1) # Exit if config is bad
    elif not os.path.exists(DB_PATH):
         print(f"WARNING: Database file '{DB_PATH}' not found.")
         print("Please run the main.py script first to create the database and select applications.")
    else:
        print(f"Starting API server, using database: {DB_PATH}")
        print(f"Swagger UI available at: http://127.0.0.1:5000/apidocs/")

    # Run Flask development server
    app.run(host='0.0.0.0', port=5000, debug=True)
































# # api.py
# import sqlite3
# import pandas as pd
# from flask import Flask, jsonify, request, Response, abort
# from flasgger import Swagger, swag_from # Import Flasgger
# import io
# import logging
# import os
# import configparser # Import configparser
# import sys # For sys.exit

# # --- Configuration ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# CONFIG_PATH = 'config.ini'

# def get_db_path_from_config(config_path=CONFIG_PATH):
#     """Reads the database path from the config file."""
#     if not os.path.exists(config_path):
#         logging.error(f"Configuration file not found: {config_path}")
#         return None # Indicate config file is missing
#     try:
#         config = configparser.ConfigParser()
#         config.read(config_path)
#         # Read db_path from [Database] section, provide a default fallback
#         db_path = config.get('Database', 'db_path', fallback='sampler_data.db')
#         return db_path
#     except (configparser.NoSectionError, configparser.NoOptionError) as e:
#          logging.error(f"Configuration error in '{config_path}': {e}")
#          return None # Indicate config error
#     except Exception as e:
#         logging.error(f"Error reading configuration: {e}")
#         return None

# DB_PATH = get_db_path_from_config()

# # --- Flask App Initialization ---
# app = Flask(__name__)

# # --- Define Schemas Directly for Swagger Config ---
# log_entry_schema = {
#     "type": "object",
#     "properties": {
#         "log_id": {"type": "integer", "example": 1},
#         "batch_id": {"type": "string", "example": "MAY2025_RUN1"},
#         "status": {"type": "string", "example": "SUCCESS"},
#         "message": {"type": "string", "example": "Selected 10 total (no flags found) from 1000 eligible (after exclusions)."},
#         "selected_count": {"type": "integer", "example": 10},
#         "total_in_pool": {"type": "integer", "example": 1000},
#         "frame_used": {"type": "integer", "example": 0},
#         "log_timestamp": {"type": "string", "format": "date-time", "example": "2025-05-04 14:00:00.123456"}
#     }
# }
# selected_app_schema = {
#     "type": "object",
#     "properties": {
#         "policy_id": {"type": "string", "format": "uuid", "example": "a1b2c3d4-e5f6-7890-1234-567890abcdef"},
#         "advisor_id": {"type": "string", "example": "ADV001"},
#         "sampling_frame_flag": {"type": "integer", "example": 1},
#         "application_receive_date": {"type": "string", "format": "date", "example": "2025-04-20"}
#     }
# }

# # Configure Flasgger with definitions
# app.config['SWAGGER'] = {
#     'title': 'Application Sampler API',
#     'uiversion': 3, # Use Swagger UI 3
#     'openapi': '3.0.2',
#     'description': 'API for retrieving application selection logs and results.',
#     # *** Add definitions directly here ***
#     'definitions': {
#         'LogEntry': log_entry_schema,
#         'SelectedApplication': selected_app_schema
#     }
#     # Add more Swagger config if needed (e.g., contact info, license)
# }
# swagger = Swagger(app) # Initialize Flasgger

# # --- Helper Function ---
# def get_db_connection():
#     """Establishes connection to the SQLite database."""
#     if DB_PATH is None: logging.error("Database path not configured."); return None
#     if not os.path.exists(DB_PATH): logging.error(f"Database file not found: {DB_PATH}"); return None
#     try:
#         conn = sqlite3.connect(DB_PATH)
#         conn.row_factory = sqlite3.Row
#         return conn
#     except sqlite3.Error as e: logging.error(f"Database connection error: {e}"); return None

# # --- API Endpoints ---

# @app.route('/status', methods=['GET'])
# @swag_from({
#     'summary': 'API Status Check',
#     'description': 'Returns the current status of the API and the configured database path.',
#     'tags': ['Status'],
#     'responses': {
#         200: {
#             'description': 'API is running.',
#             'content': { 'application/json': { 'schema': {
#                 'type': 'object',
#                 'properties': {
#                     'status': {'type': 'string', 'example': 'API is running'},
#                     'database_path': {'type': 'string', 'example': 'sampler_data.db'}
#                 }}}}
#         },
#         503: {'description': 'Database path not configured or file not found.'}
#     }
# })
# def status():
#     """API Status Check Endpoint."""
#     if DB_PATH is None or not os.path.exists(DB_PATH):
#          abort(503, description=f"Database path not configured or file missing. Path: {DB_PATH}")
#     return jsonify({"status": "API is running", "database_path": DB_PATH})

# @app.route('/api/logs', methods=['GET'])
# @swag_from({
#     'summary': 'Get All Selection Logs',
#     'description': 'Retrieves all entries from the selection log table, ordered by timestamp descending.',
#     'tags': ['Logs'],
#     'responses': {
#         200: {
#             'description': 'A list of log entries.',
#             # *** Update $ref path to definitions ***
#             'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/definitions/LogEntry'}}}}
#         },
#         404: {'description': 'Log table not found or empty.'},
#         500: {'description': 'Internal server error fetching logs.'},
#         503: {'description': 'Database service unavailable.'}
#     }
# })
# def get_logs():
#     """Retrieve all logs from the selection_log table."""
#     conn = get_db_connection()
#     if conn is None: abort(503, description="Database service unavailable.")
#     try:
#         logs_df = pd.read_sql_query("SELECT * FROM selection_log ORDER BY log_timestamp DESC", conn)
#         conn.close()
#         logs_df['log_timestamp'] = logs_df['log_timestamp'].astype(str)
#         return jsonify(logs_df.to_dict(orient='records'))
#     except pd.io.sql.DatabaseError as e:
#          logging.warning(f"Could not read logs table: {e}"); conn.close()
#          abort(404, description="Log table not found or empty.")
#     except Exception as e:
#         logging.error(f"Error fetching logs: {e}")
#         if conn: conn.close(); abort(500, description="Internal server error fetching logs.")


# @app.route('/api/selected/<string:batch_id>/json', methods=['GET'])
# @swag_from({
#     'summary': 'Get Selected Applications by Batch (JSON)',
#     'description': 'Retrieves details of applications selected in a specific batch run.',
#     'tags': ['Selections'],
#     'parameters': [
#         { 'name': 'batch_id', 'in': 'path', 'required': True, 'description': 'The Batch ID to retrieve.', 'schema': {'type': 'string'} }
#     ],
#     'responses': {
#         200: {
#             'description': 'A list of selected application details.',
#             # *** Update $ref path to definitions ***
#             'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/definitions/SelectedApplication'}}}}
#         },
#         404: {'description': 'Batch ID not found or no applications selected for this batch.'},
#         500: {'description': 'Internal server error fetching data.'},
#         503: {'description': 'Database service unavailable.'}
#     }
# })
# def get_selected_by_batch_json(batch_id):
#     """Retrieve selected applications for a specific batch as JSON."""
#     conn = get_db_connection()
#     if conn is None: abort(503, description="Database service unavailable.")
#     try:
#         query = """
#             SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
#                    sa.application_receive_date
#             FROM selected_applications sa
#             JOIN selection_log sl ON sa.selection_log_id = sl.log_id
#             WHERE sl.batch_id = ? ORDER BY sa.policy_id;
#         """
#         selected_df = pd.read_sql_query(query, conn, params=(batch_id,))
#         conn.close()
#         if selected_df.empty:
#             abort(404, description=f"No selected applications found for Batch ID: {batch_id}")
#         return jsonify(selected_df.to_dict(orient='records'))
#     except pd.io.sql.DatabaseError as e:
#          logging.warning(f"Could not read selected_applications table: {e}")
#          conn.close(); abort(404, description="Selected applications table access error.")
#     except Exception as e:
#         logging.error(f"Error fetching selected apps for batch {batch_id}: {e}")
#         if conn: conn.close(); abort(500, description="Internal server error fetching data.")


# @app.route('/api/selected/<string:batch_id>/excel', methods=['GET'])
# @swag_from({
#     'summary': 'Download Selected Applications by Batch (Excel)',
#     'description': 'Downloads an Excel file containing details of applications selected in a specific batch run.',
#     'tags': ['Selections'],
#     'parameters': [
#         { 'name': 'batch_id', 'in': 'path', 'required': True, 'description': 'The Batch ID to retrieve.', 'schema': {'type': 'string'} }
#     ],
#     'responses': {
#         200: {
#             'description': 'An Excel file download.',
#             'content': {'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {}}
#         },
#         404: {'description': 'Batch ID not found or no applications selected for this batch.'},
#         500: {'description': 'Internal server error generating file.'},
#         503: {'description': 'Database service unavailable.'}
#     }
# })
# def get_selected_by_batch_excel(batch_id):
#     """Retrieve selected applications for a specific batch as an Excel file."""
#     conn = get_db_connection()
#     if conn is None: abort(503, description="Database service unavailable.")
#     try:
#         query = """
#             SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
#                    sa.application_receive_date, sl.log_timestamp as selection_timestamp
#             FROM selected_applications sa
#             JOIN selection_log sl ON sa.selection_log_id = sl.log_id
#             WHERE sl.batch_id = ? ORDER BY sa.policy_id;
#         """
#         selected_df = pd.read_sql_query(query, conn, params=(batch_id,))
#         conn.close()
#         if selected_df.empty:
#             abort(404, description=f"No selected applications found for Batch ID: {batch_id}")

#         output = io.BytesIO()
#         with pd.ExcelWriter(output, engine='openpyxl') as writer:
#             selected_df.to_excel(writer, index=False, sheet_name='Selected Applications')
#         output.seek(0)
#         filename = f"selected_apps_{batch_id}.xlsx"
#         return Response(
#             output,
#             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#             headers={'Content-Disposition': f'attachment;filename={filename}'}
#         )
#     except pd.io.sql.DatabaseError as e:
#          logging.warning(f"Could not read selected_applications table for Excel: {e}")
#          conn.close(); abort(404, description="Selected applications table access error.")
#     except Exception as e:
#         logging.error(f"Error generating Excel for batch {batch_id}: {e}")
#         if conn: conn.close(); abort(500, description="Internal server error generating file.")

# # --- Remove schema registration function ---
# # def register_swagger_schemas():
# #     ... # Removed this function

# # --- Run the Flask App ---
# if __name__ == '__main__':
#     # --- Remove direct call to schema registration ---
#     # with app.app_context():
#     #     register_swagger_schemas() # Removed this call

#     # Check configuration and DB existence before starting
#     if DB_PATH is None:
#          print("ERROR: Database path could not be determined from config file. API cannot start.")
#          sys.exit(1) # Exit if config is bad
#     elif not os.path.exists(DB_PATH):
#          print(f"WARNING: Database file '{DB_PATH}' not found.")
#          print("Please run the main.py script first to create the database and select applications.")
#     else:
#         print(f"Starting API server, using database: {DB_PATH}")
#         print(f"Swagger UI available at: http://127.0.0.1:5000/apidocs/")

#     # Run Flask development server
#     app.run(host='0.0.0.0', port=5000, debug=True)

# api.py
# import sqlite3
# import pandas as pd
# from flask import Flask, jsonify, request, Response, abort
# from flasgger import Swagger, swag_from # Import Flasgger
# import io
# import logging
# import os
# import configparser # Import configparser
# import sys # For sys.exit

# # --- Configuration ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# CONFIG_PATH = 'config.ini'

# def get_db_path_from_config(config_path=CONFIG_PATH):
#     """Reads the database path from the config file."""
#     if not os.path.exists(config_path):
#         logging.error(f"Configuration file not found: {config_path}")
#         return None # Indicate config file is missing
#     try:
#         config = configparser.ConfigParser()
#         config.read(config_path)
#         # Read db_path from [Database] section, provide a default fallback
#         db_path = config.get('Database', 'db_path', fallback='sampler_data.db')
#         return db_path
#     except (configparser.NoSectionError, configparser.NoOptionError) as e:
#          logging.error(f"Configuration error in '{config_path}': {e}")
#          return None # Indicate config error
#     except Exception as e:
#         logging.error(f"Error reading configuration: {e}")
#         return None

# DB_PATH = get_db_path_from_config()

# # --- Flask App Initialization ---
# app = Flask(__name__)

# # --- Define Schemas Directly for Swagger Config ---
# log_entry_schema = {
#     "type": "object",
#     "properties": {
#         "log_id": {"type": "integer", "example": 1},
#         "batch_id": {"type": "string", "example": "MAY2025_RUN1"},
#         "status": {"type": "string", "example": "SUCCESS"},
#         "message": {"type": "string", "example": "Selected 10 total (no flags found) from 1000 eligible (after exclusions)."},
#         "selected_count": {"type": "integer", "example": 10},
#         "total_in_pool": {"type": "integer", "example": 1000},
#         "frame_used": {"type": "integer", "example": 0},
#         "log_timestamp": {"type": "string", "format": "date-time", "example": "2025-05-04 14:00:00.123456"}
#     }
# }
# selected_app_schema = {
#     "type": "object",
#     "properties": {
#         "policy_id": {"type": "string", "format": "uuid", "example": "a1b2c3d4-e5f6-7890-1234-567890abcdef"},
#         "advisor_id": {"type": "string", "example": "ADV001"},
#         "sampling_frame_flag": {"type": "integer", "example": 1},
#         "application_receive_date": {"type": "string", "format": "date", "example": "2025-04-20"}
#     }
# }

# # Configure Flasgger with definitions under components/schemas structure
# app.config['SWAGGER'] = {
#     'title': 'Application Sampler API',
#     'uiversion': 3, # Use Swagger UI 3
#     'openapi': '3.0.2',
#     'description': 'API for retrieving application selection logs and results.',
#     # *** Define schemas under components/schemas for OpenAPI 3 standard ***
#     'components': {
#         'schemas': {
#             'LogEntry': log_entry_schema,
#             'SelectedApplication': selected_app_schema
#         }
#     }
#     # Add more Swagger config if needed (e.g., contact info, license)
# }
# swagger = Swagger(app) # Initialize Flasgger

# # --- Helper Function ---
# def get_db_connection():
#     """Establishes connection to the SQLite database."""
#     if DB_PATH is None: logging.error("Database path not configured."); return None
#     if not os.path.exists(DB_PATH): logging.error(f"Database file not found: {DB_PATH}"); return None
#     try:
#         conn = sqlite3.connect(DB_PATH)
#         conn.row_factory = sqlite3.Row
#         return conn
#     except sqlite3.Error as e: logging.error(f"Database connection error: {e}"); return None

# # --- API Endpoints ---

# @app.route('/status', methods=['GET'])
# @swag_from({
#     'summary': 'API Status Check',
#     'description': 'Returns the current status of the API and the configured database path.',
#     'tags': ['Status'],
#     'responses': {
#         200: {
#             'description': 'API is running.',
#             'content': { 'application/json': { 'schema': {
#                 'type': 'object',
#                 'properties': {
#                     'status': {'type': 'string', 'example': 'API is running'},
#                     'database_path': {'type': 'string', 'example': 'sampler_data.db'}
#                 }}}}
#         },
#         503: {'description': 'Database path not configured or file not found.'}
#     }
# })
# def status():
#     """API Status Check Endpoint."""
#     if DB_PATH is None or not os.path.exists(DB_PATH):
#          abort(503, description=f"Database path not configured or file missing. Path: {DB_PATH}")
#     return jsonify({"status": "API is running", "database_path": DB_PATH})

# @app.route('/api/logs', methods=['GET'])
# @swag_from({
#     'summary': 'Get All Selection Logs',
#     'description': 'Retrieves all entries from the selection log table, ordered by timestamp descending.',
#     'tags': ['Logs'],
#     'responses': {
#         200: {
#             'description': 'A list of log entries.',
#             # *** Update $ref path to components/schemas ***
#             'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/components/schemas/LogEntry'}}}}
#         },
#         404: {'description': 'Log table not found or empty.'},
#         500: {'description': 'Internal server error fetching logs.'},
#         503: {'description': 'Database service unavailable.'}
#     }
# })
# def get_logs():
#     """Retrieve all logs from the selection_log table."""
#     conn = get_db_connection()
#     if conn is None: abort(503, description="Database service unavailable.")
#     try:
#         logs_df = pd.read_sql_query("SELECT * FROM selection_log ORDER BY log_timestamp DESC", conn)
#         conn.close()
#         logs_df['log_timestamp'] = logs_df['log_timestamp'].astype(str)
#         return jsonify(logs_df.to_dict(orient='records'))
#     except pd.io.sql.DatabaseError as e:
#          logging.warning(f"Could not read logs table: {e}"); conn.close()
#          abort(404, description="Log table not found or empty.")
#     except Exception as e:
#         logging.error(f"Error fetching logs: {e}")
#         if conn: conn.close(); abort(500, description="Internal server error fetching logs.")


# @app.route('/api/selected/<string:batch_id>/json', methods=['GET'])
# @swag_from({
#     'summary': 'Get Selected Applications by Batch (JSON)',
#     'description': 'Retrieves details of applications selected in a specific batch run.',
#     'tags': ['Selections'],
#     'parameters': [
#         { 'name': 'batch_id', 'in': 'path', 'required': True, 'description': 'The Batch ID to retrieve.', 'schema': {'type': 'string'} }
#     ],
#     'responses': {
#         200: {
#             'description': 'A list of selected application details.',
#             # *** Update $ref path to components/schemas ***
#             'content': { 'application/json': { 'schema': {'type': 'array', 'items': {'$ref': '#/components/schemas/SelectedApplication'}}}}
#         },
#         404: {'description': 'Batch ID not found or no applications selected for this batch.'},
#         500: {'description': 'Internal server error fetching data.'},
#         503: {'description': 'Database service unavailable.'}
#     }
# })
# def get_selected_by_batch_json(batch_id):
#     """Retrieve selected applications for a specific batch as JSON."""
#     conn = get_db_connection()
#     if conn is None: abort(503, description="Database service unavailable.")
#     try:
#         query = """
#             SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
#                    sa.application_receive_date
#             FROM selected_applications sa
#             JOIN selection_log sl ON sa.selection_log_id = sl.log_id
#             WHERE sl.batch_id = ? ORDER BY sa.policy_id;
#         """
#         selected_df = pd.read_sql_query(query, conn, params=(batch_id,))
#         conn.close()
#         if selected_df.empty:
#             abort(404, description=f"No selected applications found for Batch ID: {batch_id}")
#         return jsonify(selected_df.to_dict(orient='records'))
#     except pd.io.sql.DatabaseError as e:
#          logging.warning(f"Could not read selected_applications table: {e}")
#          conn.close(); abort(404, description="Selected applications table access error.")
#     except Exception as e:
#         logging.error(f"Error fetching selected apps for batch {batch_id}: {e}")
#         if conn: conn.close(); abort(500, description="Internal server error fetching data.")


# @app.route('/api/selected/<string:batch_id>/excel', methods=['GET'])
# @swag_from({
#     'summary': 'Download Selected Applications by Batch (Excel)',
#     'description': 'Downloads an Excel file containing details of applications selected in a specific batch run.',
#     'tags': ['Selections'],
#     'parameters': [
#         { 'name': 'batch_id', 'in': 'path', 'required': True, 'description': 'The Batch ID to retrieve.', 'schema': {'type': 'string'} }
#     ],
#     'responses': {
#         200: {
#             'description': 'An Excel file download.',
#             'content': {'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {}}
#         },
#         404: {'description': 'Batch ID not found or no applications selected for this batch.'},
#         500: {'description': 'Internal server error generating file.'},
#         503: {'description': 'Database service unavailable.'}
#     }
# })
# def get_selected_by_batch_excel(batch_id):
#     """Retrieve selected applications for a specific batch as an Excel file."""
#     conn = get_db_connection()
#     if conn is None: abort(503, description="Database service unavailable.")
#     try:
#         query = """
#             SELECT sa.policy_id, sa.advisor_id, sa.sampling_frame_flag,
#                    sa.application_receive_date, sl.log_timestamp as selection_timestamp
#             FROM selected_applications sa
#             JOIN selection_log sl ON sa.selection_log_id = sl.log_id
#             WHERE sl.batch_id = ? ORDER BY sa.policy_id;
#         """
#         selected_df = pd.read_sql_query(query, conn, params=(batch_id,))
#         conn.close()
#         if selected_df.empty:
#             abort(404, description=f"No selected applications found for Batch ID: {batch_id}")

#         output = io.BytesIO()
#         with pd.ExcelWriter(output, engine='openpyxl') as writer:
#             selected_df.to_excel(writer, index=False, sheet_name='Selected Applications')
#         output.seek(0)
#         filename = f"selected_apps_{batch_id}.xlsx"
#         return Response(
#             output,
#             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#             headers={'Content-Disposition': f'attachment;filename={filename}'}
#         )
#     except pd.io.sql.DatabaseError as e:
#          logging.warning(f"Could not read selected_applications table for Excel: {e}")
#          conn.close(); abort(404, description="Selected applications table access error.")
#     except Exception as e:
#         logging.error(f"Error generating Excel for batch {batch_id}: {e}")
#         if conn: conn.close(); abort(500, description="Internal server error generating file.")


# # --- Run the Flask App ---
# if __name__ == '__main__':
#     # Check configuration and DB existence before starting
#     if DB_PATH is None:
#          print("ERROR: Database path could not be determined from config file. API cannot start.")
#          sys.exit(1) # Exit if config is bad
#     elif not os.path.exists(DB_PATH):
#          print(f"WARNING: Database file '{DB_PATH}' not found.")
#          print("Please run the main.py script first to create the database and select applications.")
#     else:
#         print(f"Starting API server, using database: {DB_PATH}")
#         print(f"Swagger UI available at: http://127.0.0.1:5000/apidocs/")

#     # Run Flask development server
#     app.run(host='0.0.0.0', port=5000, debug=True)

