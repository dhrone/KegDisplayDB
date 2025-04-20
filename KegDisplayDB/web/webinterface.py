from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import sqlite3
import os
import bcrypt
from functools import wraps
from datetime import datetime, UTC
import logging
import argparse
import sys
import csv
import io
import subprocess
import shutil
from pathlib import Path
import threading
import uuid
import time

# Import log configuration
from ..utils.log_config import configure_logging

# Import the SyncedDatabase and DatabaseManager
from ..db import SyncedDatabase
from ..db.database import DatabaseManager

# Global variable to track import status
import_status = {
    "in_progress": False,
    "last_import": {
        "timestamp": None,
        "success": None,
        "imported_count": 0,
        "errors": []
    }
}

# Import Gunicorn at module level
try:
    from gunicorn.app.base import BaseApplication
except ImportError:
    # We'll handle this error when the start function is called
    BaseApplication = None

# Define paths and directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(USER_HOME, ".KegDisplayDB")
DATA_DIR = os.path.join(CONFIG_DIR, "data")
ETC_DIR = os.path.join(CONFIG_DIR, "etc")
SSL_DIR = os.path.join(CONFIG_DIR, "ssl")

# Create required directories if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ETC_DIR, exist_ok=True)
os.makedirs(SSL_DIR, exist_ok=True)

# Define paths to database and config files
DB_PATH = os.path.join(DATA_DIR, 'beer.db')
PASSWD_PATH = os.path.join(ETC_DIR, 'passwd')
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
print(f"Password file path: {PASSWD_PATH}")
print(f"Template directory: {TEMPLATE_DIR}")
print(f"Database path: {DB_PATH}")

# Get the pre-configured logger
logger = logging.getLogger("KegDisplay")

# Define default arguments
DEFAULT_ARGS = {
    'host': '0.0.0.0',
    'port': 8080,
    'broadcast_port': 5002,
    'sync_port': 5003,
    'no_sync': False,
    'debug': False,
    'log_level': 'INFO',
    'ssl_cert': os.path.join(SSL_DIR, 'certs', 'kegdisplay.crt'),
    'ssl_key': os.path.join(SSL_DIR, 'private', 'kegdisplay.key'),
    'workers': 2,
    'worker_class': 'sync',
    'timeout': 30
}

# Create a namespace object with default arguments
class Args:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

# Initialize with default arguments
args = Args(**DEFAULT_ARGS)

def parse_args(argv=None):
    """
    Parse command line arguments.
    
    Args:
        argv: Command line arguments (default: None, using sys.argv)
        
    Returns:
        Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description='KegDisplay Web Interface')
    parser.add_argument('--host', default=DEFAULT_ARGS['host'], help='Host to listen on')
    parser.add_argument('--port', type=int, default=DEFAULT_ARGS['port'], help='Port to listen on')
    parser.add_argument('--broadcast-port', type=int, default=DEFAULT_ARGS['broadcast_port'], 
                        help='UDP port for synchronization broadcasts (default: 5002)')
    parser.add_argument('--sync-port', type=int, default=DEFAULT_ARGS['sync_port'],
                        help='TCP port for synchronization connections (default: 5003)')
    parser.add_argument('--no-sync', action='store_true',
                        help='Disable database synchronization')
    parser.add_argument('--debug', action='store_true',
                        help='Run Flask in debug mode')
    parser.add_argument('--log-level', 
                        default=DEFAULT_ARGS['log_level'],
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        type=str.upper,
                        help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    # Add SSL-related arguments
    parser.add_argument('--ssl-cert', type=str,
                        default=DEFAULT_ARGS['ssl_cert'],
                        help='Path to SSL certificate file (default: ~/.KegDisplayDB/ssl/certs/kegdisplay.crt)')
    parser.add_argument('--ssl-key', type=str,
                        default=DEFAULT_ARGS['ssl_key'],
                        help='Path to SSL private key file (default: ~/.KegDisplayDB/ssl/private/kegdisplay.key)')
    parser.add_argument('--workers', type=int, default=DEFAULT_ARGS['workers'],
                        help='Number of Gunicorn worker processes (default: 2)')
    parser.add_argument('--worker-class', type=str, default=DEFAULT_ARGS['worker_class'],
                        choices=['sync', 'eventlet', 'gevent'],
                        help='Gunicorn worker class (default: sync)')
    parser.add_argument('--timeout', type=int, default=DEFAULT_ARGS['timeout'],
                        help='Worker timeout in seconds (default: 30)')
    return parser.parse_args(argv)

# Parse command line arguments at module level
# This ensures args are available when imported by another module
# Skip parsing args if not being run directly - this avoids arg parsing during imports/tests
if not hasattr(sys, '_called_from_test') and 'pytest' not in sys.modules:
    try:
        args = parse_args()
    except SystemExit:
        # This can happen when --help is passed or invalid arguments are provided
        # Keep using default args in this case
        pass

# Initialize the SyncedDatabase unless disabled
synced_db = None
# Only initialize at module level if called directly, not when imported
if not hasattr(sys, '_called_from_test') and 'pytest' not in sys.modules and __name__ == '__main__':
    try:
        print(f"Initializing SyncedDatabase with broadcast_port={args.broadcast_port}, sync_port={args.sync_port}")
        synced_db = SyncedDatabase(
            db_path=DB_PATH,
            broadcast_port=args.broadcast_port,
            sync_port=args.sync_port,
            test_mode=False
        )
        logger.info("Initialized SyncedDatabase for web interface")
    except OSError as e:
        print(f"Error initializing SyncedDatabase: {e}")
        print("If another instance is already running, use --broadcast-port and --sync-port to set different ports")
        print("or use --no-sync to disable synchronization for this instance.")
        sys.exit(1)

# Remove db_manager initialization since we only use synced_db now
# We still need to handle the case where synced_db is None in API methods

app = Flask(__name__, 
           template_folder=TEMPLATE_DIR)  # Specify the template folder
app.secret_key = os.urandom(24)  # Generate a random secret key
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, username, password_hash=None):
        self.id = username
        self.username = username
        self.password_hash = password_hash

    @staticmethod
    def get(user_id):
        users = load_users()
        if user_id in users:
            return User(user_id, users[user_id])
        return None

def load_users():
    users = {}
    print("Loading Users")
    try:
        with open(PASSWD_PATH, 'r') as f:
            for line in f:
                username, password_hash = line.strip().split(':')
                users[username] = password_hash
                print(f"Found user: {username}")
    except FileNotFoundError:
        print(f"Password file not found at: {PASSWD_PATH}")
    return users

@login_manager.user_loader
def load_user(user_id):
    print(f"Loading user with ID: {user_id}")
    return User.get(user_id)

def get_db_tables():
    """Get all table names from the database"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            logger.error("Database synchronization service not available")
            return []
            
        with synced_db.db_manager.transaction() as conn:
            tables = synced_db.db_manager.query("SELECT name FROM sqlite_master WHERE type='table';", conn=conn)
        return [table[0] for table in tables]
    except Exception as e:
        logger.error(f"Error getting database tables: {e}")
        return []

def get_table_schema(table_name):
    """Get the schema for a specific table"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            logger.error("Database synchronization service not available")
            return []
            
        with synced_db.db_manager.transaction() as conn:
            schema = synced_db.db_manager.query(f"PRAGMA table_info({table_name});", conn=conn)
        return schema
    except Exception as e:
        logger.error(f"Error getting schema for table {table_name}: {e}")
        return []

def get_table_data(table_name):
    """Get all data and column names from a specific table"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            logger.error("Database synchronization service not available")
            return [], []
            
        with synced_db.db_manager.transaction() as conn:
            data = synced_db.db_manager.query(f"SELECT * FROM {table_name};", conn=conn)
            schema = get_table_schema(table_name)
            columns = [col[1] for col in schema]
        return columns, data
    except Exception as e:
        logger.error(f"Error getting data from table {table_name}: {e}")
        return [], []

@app.route('/')
@login_required
def index():
    # Redirect to the taps page
    return redirect(url_for('taps'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    print("Entering login route")
    print(f"Template folder: {app.template_folder}")
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        print(f"Login attempt for user: {username}")
        
        users = load_users()
        print(f"Users in passwd file: {users}")  # This will show us what users are loaded
        
        if username in users:
            try:
                stored_hash = users[username]
                print(f"Stored hash for {username}: {stored_hash}")
                print(f"Attempting to verify password")
                result = bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
                print(f"Password verification result: {result}")
                
                if result:
                    user = User(username, stored_hash)
                    login_user(user)
                    print(f"Login successful for user: {username}")
                    return redirect(url_for('index'))
                else:
                    print(f"Invalid password for user: {username}")
            except Exception as e:
                print(f"Error during password verification: {str(e)}")
                import traceback
                print(traceback.format_exc())
        else:
            print(f"User not found: {username}")
        
        flash('Invalid username or password')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/taps')
@login_required
def taps():
    return render_template('taps.html', active_page='taps')

@app.route('/beers')
@login_required
def beers():
    return render_template('beers.html', active_page='beers')

@app.route('/dbmanage')
@login_required
def db_manage():
    return render_template('dbmanage.html', active_page='dbmanage')

@app.route('/api/beers/backup', methods=['GET'])
@login_required
def backup_beers():
    """Create a CSV backup of all beers from the database"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        # Get all beers from the database
        beers = synced_db.get_all_beers()
        
        # Get column names from the first beer or use predefined columns
        if beers:
            columns = beers[0].keys()
        else:
            columns = ['idBeer', 'Name', 'ABV', 'IBU', 'Color', 'OriginalGravity', 'FinalGravity',
                    'Description', 'Brewed', 'Kegged', 'Tapped', 'Notes']
        
        # Create a CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(columns)
        
        # Write data
        for beer in beers:
            writer.writerow([beer.get(col) for col in columns])
        
        # Prepare response
        csv_content = output.getvalue()
        output.close()
        
        response = app.response_class(
            response=csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=beers_backup.csv'}
        )
        
        return response
    except Exception as e:
        logger.error(f"Error creating beer backup: {e}")
        return jsonify({"error": f"Failed to create backup: {str(e)}"}), 500

@app.route('/api/beers/import', methods=['POST'])
@login_required
def import_beers():
    global import_status
    
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "File must be a CSV"}), 400
    
    # Update global import status
    import_status["in_progress"] = True
    import_status["last_import"] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "success": None,
        "imported_count": 0,
        "errors": [],
        "status": "Downloading file..."
    }
    
    # Define a temporary file path for storing the uploaded CSV
    temp_file_path = os.path.join(DATA_DIR, f"temp_import_{uuid.uuid4()}.csv")
    
    def background_import():
        global import_status
        logger = logging.getLogger("KegDisplay")
        try:
            logger.info("Starting background beer import process")
            
            # Step 1: Read and save the uploaded file in chunks
            try:
                with open(temp_file_path, 'wb') as temp_file:
                    chunk_size = 1024  # 1KB chunks
                    while True:
                        chunk = file.stream.read(chunk_size)
                        if not chunk:
                            break
                        temp_file.write(chunk)
                
                import_status["last_import"]["status"] = "Processing CSV data..."
                logger.info(f"Uploaded file saved to temporary location: {temp_file_path}")
            except Exception as e:
                logger.error(f"Error saving uploaded file: {e}")
                import_status["in_progress"] = False
                import_status["last_import"]["success"] = False
                import_status["last_import"]["errors"] = [f"Error saving file: {str(e)}"]
                import_status["last_import"]["status"] = "Failed"
                return
            
            # Step 2: Verify and parse the CSV file
            try:
                with open(temp_file_path, 'r', newline='') as csv_file:
                    reader = csv.DictReader(csv_file)
                    
                    # Check if required fields are present
                    required_field = 'Name'
                    if required_field not in reader.fieldnames:
                        raise ValueError(f"Required field '{required_field}' is missing from CSV")
                    
                    # Convert CSV rows to a list of dictionaries
                    beer_data_list = []
                    for row in reader:
                        # Skip rows without a name
                        if not row.get('Name'):
                            continue
                            
                        # Convert empty strings to None for numeric fields
                        for field in ['ABV', 'IBU', 'Color', 'OriginalGravity', 'FinalGravity']:
                            if field in row and (not row[field] or row[field].strip() == ''):
                                row[field] = None
                            elif field in row:
                                try:
                                    row[field] = float(row[field])
                                except (ValueError, TypeError):
                                    row[field] = None
                        
                        beer_data_list.append(row)
                    
                    if not beer_data_list:
                        raise ValueError("No valid beer data found in the CSV")
                    
                    logger.info(f"Successfully parsed {len(beer_data_list)} beer entries from CSV")
                    import_status["last_import"]["status"] = f"Importing {len(beer_data_list)} beers to database..."
                    
            except Exception as e:
                logger.error(f"Error parsing CSV file: {e}")
                import_status["in_progress"] = False
                import_status["last_import"]["success"] = False
                import_status["last_import"]["errors"] = [f"Error parsing CSV: {str(e)}"]
                import_status["last_import"]["status"] = "Failed"
                return
            
            # Step 3 & 4: Import the beer data using the helper function
            try:
                if synced_db:
                    # Call the import function with the entire data list
                    import_status["last_import"]["status"] = "Importing to database..."
                    imported_count, errors = synced_db.import_beers_from_data(beer_data_list)
                    
                    import_status["last_import"]["imported_count"] = imported_count
                    import_status["last_import"]["errors"] = errors[:10]  # Limit to first 10 errors
                    import_status["last_import"]["success"] = True
                    import_status["last_import"]["status"] = f"Import complete: {imported_count} beers imported"
                    
                    logger.info(f"Background import completed: {imported_count} beers imported with {len(errors)} errors")
                else:
                    import_status["last_import"]["success"] = False
                    import_status["last_import"]["errors"] = ["Database synchronization service not available"]
                    import_status["last_import"]["status"] = "Failed: Database service unavailable"
                    logger.error("Cannot import: synced_db is not available")
            except Exception as e:
                logger.error(f"Error during database import: {e}")
                import_status["last_import"]["success"] = False
                import_status["last_import"]["errors"] = [f"Database error: {str(e)}"]
                import_status["last_import"]["status"] = "Failed during database import"
            
            # Clean up the temporary file
            try:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                    logger.info(f"Removed temporary import file: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Could not remove temporary file {temp_file_path}: {e}")
            
            # Mark import as complete
            import_status["in_progress"] = False
            import_status["last_import"]["timestamp"] = datetime.now(UTC).isoformat()
            
        except Exception as e:
            logger.error(f"Unexpected error in background import: {e}")
            import_status["in_progress"] = False
            import_status["last_import"]["timestamp"] = datetime.now(UTC).isoformat()
            import_status["last_import"]["success"] = False
            import_status["last_import"]["errors"] = [f"Unexpected error: {str(e)}"]
            import_status["last_import"]["status"] = "Failed with an unexpected error"
            
            # Try to clean up
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
    
    # Start the background import thread
    import_thread = threading.Thread(target=background_import, daemon=True)
    import_thread.start()
    
    # Return success immediately with background process info
    return jsonify({
        "success": True,
        "message": "Import started in the background. Check import status for updates."
    })

@app.route('/api/beers/import-status', methods=['GET'])
@login_required
def get_import_status():
    """Get the current status of beer import operations"""
    global import_status
    
    # Add a timestamp to the response
    response = {
        "in_progress": import_status["in_progress"],
        "current_time": datetime.now(UTC).isoformat()
    }
    
    # Include last import details if available
    if import_status["last_import"]["timestamp"]:
        response["last_import"] = import_status["last_import"]
    
    return jsonify(response)

@app.route('/api/beers/clear', methods=['POST'])
@login_required
def clear_beers():
    """Clear all beers from the database"""
    # Check for confirmation
    confirmation = request.json.get('confirmation')
    if not confirmation or confirmation != 'CONFIRM':
        return jsonify({"error": "Confirmation required"}), 400
    
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        # Use the synced_db.clear_all_beers method which handles transactions internally
        beer_count = synced_db.clear_all_beers()
        
        return jsonify({
            "success": True,
            "message": f"Successfully cleared {beer_count} beers from database"
        })
    
    except Exception as e:
        logger.error(f"Error clearing beers: {e}")
        return jsonify({"error": f"Error clearing beers: {str(e)}"}), 500

@app.route('/api/taps', methods=['GET'])
@login_required
def api_get_taps():
    """Get all taps with beer information"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        # Get all taps using SyncedDatabase
        taps = synced_db.get_all_taps()
        
        # Enhance with additional beer information
        for tap in taps:
            if tap['idBeer']:
                beer = synced_db.get_beer(tap['idBeer'])
                if beer:
                    tap['BeerName'] = beer['Name']
                    tap['ABV'] = beer['ABV']
                    tap['IBU'] = beer['IBU'] 
                    tap['Description'] = beer['Description']
                    
        return jsonify(taps)
    except Exception as e:
        logger.error(f"Error getting taps: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['GET'])
@login_required
def api_get_tap(tap_id):
    """Get a specific tap with beer information"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        tap = synced_db.get_tap(tap_id)
        
        if tap and tap['idBeer']:
            beer = synced_db.get_beer(tap['idBeer'])
            if beer:
                tap['BeerName'] = beer['Name']
                tap['ABV'] = beer['ABV']
                tap['IBU'] = beer['IBU']
                tap['Description'] = beer['Description']
                
        if tap:
            return jsonify(tap)
        else:
            return jsonify({"error": "Tap not found"}), 404
    except Exception as e:
        logger.error(f"Error getting tap {tap_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps', methods=['POST'])
@login_required
def api_add_tap():
    """Add a new tap"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        data = request.json
        
        # Get beer_id if provided, otherwise use NULL
        beer_id = data.get('idBeer')
        
        # Get the next available tap ID
        existing_taps = synced_db.get_all_taps()
        tap_ids = [tap['idTap'] for tap in existing_taps]
        next_tap_id = 1 if not tap_ids else max(tap_ids) + 1
        
        # Add the tap using SyncedDatabase
        tap_id = synced_db.add_tap(next_tap_id, beer_id)
        
        # Get the new tap with beer info
        tap = synced_db.get_tap(tap_id)
        
        # Add beer info if applicable
        if beer_id:
            beer = synced_db.get_beer(beer_id)
            if beer:
                tap['BeerName'] = beer['Name']
                tap['ABV'] = beer['ABV'] 
                tap['IBU'] = beer['IBU']
                tap['Description'] = beer['Description']
        
        return jsonify(tap), 201
    except Exception as e:
        logger.error(f"Error adding tap: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['PUT'])
@login_required
def api_update_tap(tap_id):
    """Update a tap's beer assignment"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        data = request.json
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        beer_id = data.get('beer_id')
        
        # Check if tap exists
        tap = synced_db.get_tap(tap_id)
                
        if not tap:
            return jsonify({"error": f"Tap #{tap_id} not found"}), 404
            
        # Update the beer assignment using synced_db
        success = synced_db.update_tap(tap_id, beer_id)
        
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update tap"}), 500
    except Exception as e:
        logger.error(f"Error updating tap {tap_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['DELETE'])
@login_required
def api_delete_tap(tap_id):
    """Delete a tap from the database"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        # Check if tap exists
        tap = synced_db.get_tap(tap_id)
                
        if not tap:
            return jsonify({"error": f"Tap #{tap_id} not found"}), 404
            
        # Delete the tap using synced_db
        success = synced_db.delete_tap(tap_id)
        
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to delete tap"}), 500
    except Exception as e:
        logger.error(f"Error deleting tap {tap_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers', methods=['GET'])
@login_required
def api_get_beers():
    """Get all beers from the database"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        beers = synced_db.get_all_beers()
        return jsonify(beers)
    except Exception as e:
        logger.error(f"Error getting beers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['GET'])
@login_required
def api_get_beer(beer_id):
    """Get a specific beer by ID"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        beer = synced_db.get_beer(beer_id)
        
        if beer:
            return jsonify(beer)
        else:
            return jsonify({"error": "Beer not found"}), 404
    except Exception as e:
        logger.error(f"Error getting beer {beer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['DELETE'])
@login_required
def api_delete_beer(beer_id):
    """Delete a beer from the database"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        # Check if beer exists
        beer = synced_db.get_beer(beer_id)
                
        if not beer:
            return jsonify({"error": "Beer not found"}), 404
            
        # Delete the beer using synced_db
        # Note: synced_db.delete_beer will automatically handle updating any taps using this beer
        success = synced_db.delete_beer(beer_id)
        
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to delete beer"}), 500
    except Exception as e:
        logger.error(f"Error deleting beer {beer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>/taps', methods=['GET'])
@login_required
def api_get_beer_taps(beer_id):
    """Get all taps that have a specific beer assigned"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        taps = synced_db.get_tap_with_beer(beer_id)
        return jsonify(taps)
    except Exception as e:
        logger.error(f"Error getting taps for beer {beer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers', methods=['POST'])
@login_required
def api_add_beer():
    """Add a new beer to the database"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        data = request.json
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        name = data.get('Name')
        
        # Validate data
        if not name:
            return jsonify({"error": "Beer name is required"}), 400
        
        # Add the beer using synced_db
        beer_data = {
            'name': name,
            'abv': data.get('ABV'),
            'ibu': data.get('IBU'),
            'color': data.get('Color'),
            'og': data.get('OriginalGravity'),
            'fg': data.get('FinalGravity'),
            'description': data.get('Description'),
            'brewed': data.get('Brewed'),
            'kegged': data.get('Kegged'),
            'tapped': data.get('Tapped'),
            'notes': data.get('Notes')
        }
        
        beer_id = synced_db.add_beer(**beer_data)
        
        if beer_id:
            return jsonify({"success": True, "beer_id": beer_id}), 201
        else:
            return jsonify({"error": "Failed to create beer"}), 500
    except Exception as e:
        logger.error(f"Error adding beer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['PUT'])
@login_required
def api_update_beer(beer_id):
    """Update an existing beer"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        data = request.json
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        name = data.get('Name')
        
        # Validate data
        if not name:
            return jsonify({"error": "Beer name is required"}), 400
        
        # Check if beer exists
        beer = synced_db.get_beer(beer_id)
        
        if not beer:
            return jsonify({"error": "Beer not found"}), 404
        
        # Update the beer using synced_db
        beer_data = {
            'beer_id': beer_id,
            'name': name,
            'abv': data.get('ABV'),
            'ibu': data.get('IBU'),
            'color': data.get('Color'),
            'og': data.get('OriginalGravity'),
            'fg': data.get('FinalGravity'),
            'description': data.get('Description'),
            'brewed': data.get('Brewed'),
            'kegged': data.get('Kegged'),
            'tapped': data.get('Tapped'),
            'notes': data.get('Notes')
        }
        
        success = synced_db.update_beer(**beer_data)
        
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update beer"}), 500
    except Exception as e:
        logger.error(f"Error updating beer {beer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/count', methods=['POST'])
@login_required
def api_set_tap_count():
    """Set the number of taps in the system"""
    try:
        # Ensure synced_db is initialized
        if synced_db is None:
            return jsonify({"error": "Database synchronization service not available"}), 503
        
        data = request.json
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        count = data.get('count')
        
        if count is None or not isinstance(count, int) or count < 1:
            return jsonify({"error": "Valid tap count is required (must be positive integer)"}), 400
        
        # Use the set_tap_count method from synced_db
        success = synced_db.set_tap_count(count)
        
        if success:
            return jsonify({"success": True, "tap_count": count})
        else:
            return jsonify({"error": "Failed to set tap count"}), 500
    except Exception as e:
        logger.error(f"Error setting tap count: {e}")
        return jsonify({"error": str(e)}), 500

def generate_self_signed_certificate(cert_path, key_path):
    """
    Generate a self-signed SSL certificate and key if they don't exist
    
    Args:
        cert_path: Path where the certificate will be saved
        key_path: Path where the private key will be saved
    
    Returns:
        bool: True if certificate was generated, False if error occurred
    """
    # Create directories if they don't exist
    cert_dir = os.path.dirname(cert_path)
    key_dir = os.path.dirname(key_path)
    os.makedirs(cert_dir, exist_ok=True) 
    os.makedirs(key_dir, exist_ok=True)
    
    try:
        # Generate private key and certificate
        logger.info(f"Generating self-signed SSL certificate: {cert_path}")
        subprocess.run([
            'openssl', 'req', '-x509', '-newkey', 'rsa:2048', 
            '-keyout', key_path, 
            '-out', cert_path,
            '-days', '365',
            '-nodes',  # No passphrase
            '-subj', '/CN=kegdisplay.local'
        ], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        
        # Set proper permissions on the key file
        os.chmod(key_path, 0o600)
        logger.info(f"Self-signed certificate generated successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to generate SSL certificate: {e.stderr.decode('utf-8')}")
        return False
    except Exception as e:
        logger.error(f"Error generating SSL certificate: {str(e)}")
        return False

class KegDisplayApplication(BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()
    
    def load_config(self):
        for key, value in self.options.items():
            self.cfg.set(key, value)
    
    def load(self):
        return self.application

def start(passed_args=None):
    """
    Start the web interface server.
    
    Args:
        passed_args: Arguments passed from another module. If None, use command line arguments.
    """
    global args
    global synced_db
    
    # Parse arguments if they weren't passed
    if passed_args is None:
        passed_args = parse_args()
    
    # Update the global args
    args = passed_args
    
    # Configure logging
    configure_logging(log_level=args.log_level)
    
    # Re-initialize logger with configured level
    logger = logging.getLogger("KegDisplay")
    logger.setLevel(getattr(logging, args.log_level))
    
    # Initialize SyncedDatabase if not disabled
    if not args.no_sync and synced_db is None:
        try:
            logger.info(f"Initializing SyncedDatabase with broadcast_port={args.broadcast_port}, sync_port={args.sync_port}")
            synced_db = SyncedDatabase(
                db_path=DB_PATH,
                broadcast_port=args.broadcast_port,
                sync_port=args.sync_port,
                test_mode=False
            )
            logger.info("Initialized SyncedDatabase for web interface")
            logger.info(f"Database synchronization active on ports {args.broadcast_port} (UDP) and {args.sync_port} (TCP)")
        except OSError as e:
            logger.error(f"Error initializing SyncedDatabase: {e}")
            logger.error("If another instance is already running, use --broadcast-port and --sync-port to set different ports")
            logger.error("or use --no-sync to disable synchronization for this instance.")
            sys.exit(1)
    elif args.no_sync:
        logger.warning("Database synchronization disabled (--no-sync flag)")
        logger.warning("The application will not be able to sync with other instances")
    
    # Check if Gunicorn is available
    if BaseApplication is None:
        logger.error("Gunicorn is not installed. Please install it with: pip install gunicorn")
        sys.exit(1)
    
    # Display configuration
    logger.info(f"Web interface configuration:")
    logger.info(f"  Host: {args.host}")
    logger.info(f"  Web port: {args.port}")
    logger.info(f"  Broadcast port: {args.broadcast_port}")
    logger.info(f"  Sync port: {args.sync_port}")
    logger.info(f"  Synchronization: {'Disabled' if args.no_sync else 'Enabled'}")
    logger.info(f"  Debug mode: {'Enabled' if args.debug else 'Disabled'}")
    
    # Configure Gunicorn options optimized for Raspberry Pi Zero 2W
    options = {
        'bind': f"{args.host}:{args.port}",
        'workers': args.workers,
        'worker_class': args.worker_class,
        'timeout': args.timeout,
        'worker_connections': 100,
        'max_requests': 1000,
        'max_requests_jitter': 50,
        'keepalive': 2,
        'graceful_timeout': 30,
        'accesslog': '-',
        'errorlog': '-',
        'loglevel': args.log_level.lower(),
        'capture_output': True,
        'enable_stdio_inheritance': True,
        'daemon': False,
        'pidfile': None,
        'umask': 0,
        'user': None,
        'group': None,
        'tmp_upload_dir': None,
        'reload': args.debug,
    }
    
    # Add SSL configuration if certificates are provided
    if args.ssl_cert and args.ssl_key:
        # Check if certificate and key files exist, generate them if not
        if not os.path.exists(args.ssl_cert) or not os.path.exists(args.ssl_key):
            logger.info("SSL certificate or key not found, generating self-signed certificate")
            if generate_self_signed_certificate(args.ssl_cert, args.ssl_key):
                logger.info("SSL certificate and key generated successfully")
            else:
                logger.error("Failed to generate SSL certificate and key")
                sys.exit(1)
        
        options['certfile'] = args.ssl_cert
        options['keyfile'] = args.ssl_key
        logger.info(f"SSL enabled with certificate: {args.ssl_cert}")
    
    # Start the Gunicorn server
    logger.info(f"Starting Gunicorn server on {args.host}:{args.port}")
    logger.info(f"Worker configuration: {args.workers} workers, {args.worker_class} worker class")
    KegDisplayApplication(app, options).run()

if __name__ == '__main__':
    # Only parse arguments when run as a script
    start(parse_args()) 
