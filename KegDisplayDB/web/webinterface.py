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

# Import log configuration
from ..utils.log_config import configure_logging

# Import the SyncedDatabase and DatabaseManager
from ..db import SyncedDatabase
from ..db.database import DatabaseManager

# Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(USER_HOME, ".KegDisplayDB")
DATA_DIR = os.path.join(CONFIG_DIR, "data")
ETC_DIR = os.path.join(CONFIG_DIR, "etc")
SSL_DIR = os.path.join(CONFIG_DIR, "ssl")

# Create required directories if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ETC_DIR, exist_ok=True)

# Define paths to database and config files
DB_PATH = os.path.join(DATA_DIR, 'beer.db')
PASSWD_PATH = os.path.join(ETC_DIR, 'passwd')
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
print(f"Password file path: {PASSWD_PATH}")
print(f"Template directory: {TEMPLATE_DIR}")
print(f"Database path: {DB_PATH}")

# Get the pre-configured logger
logger = logging.getLogger("KegDisplay")

# Parse all command line arguments at the beginning
def parse_args():
    parser = argparse.ArgumentParser(description='KegDisplay Web Interface')
    parser.add_argument('--host', default='0.0.0.0', help='Host to listen on')
    parser.add_argument('--port', type=int, default=8080, help='Port to listen on')
    parser.add_argument('--broadcast-port', type=int, default=5002, 
                       help='UDP port for synchronization broadcasts (default: 5002)')
    parser.add_argument('--sync-port', type=int, default=5003,
                       help='TCP port for synchronization connections (default: 5003)')
    parser.add_argument('--no-sync', action='store_true',
                       help='Disable database synchronization')
    parser.add_argument('--debug', action='store_true',
                       help='Run Flask in debug mode')
    parser.add_argument('--log-level', 
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       type=str.upper,
                       help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    # Add SSL-related arguments
    parser.add_argument('--ssl-cert', type=str,
                       default=os.path.join(SSL_DIR, 'certs', 'kegdisplay.crt'),
                       help='Path to SSL certificate file (default: ~/.KegDisplayDB/ssl/certs/kegdisplay.crt)')
    parser.add_argument('--ssl-key', type=str,
                       default=os.path.join(SSL_DIR, 'private', 'kegdisplay.key'),
                       help='Path to SSL private key file (default: ~/.KegDisplayDB/ssl/private/kegdisplay.key)')
    parser.add_argument('--workers', type=int, default=2,
                       help='Number of Gunicorn worker processes (default: 2)')
    parser.add_argument('--worker-class', type=str, default='sync',
                       choices=['sync', 'eventlet', 'gevent'],
                       help='Gunicorn worker class (default: sync)')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Worker timeout in seconds (default: 30)')
    return parser.parse_args()

# Parse arguments once at module level
args = parse_args()
print(f"Web interface configuration:")
print(f"  Host: {args.host}")
print(f"  Web port: {args.port}")
print(f"  Broadcast port: {args.broadcast_port}")
print(f"  Sync port: {args.sync_port}")
print(f"  Synchronization: {'Disabled' if args.no_sync else 'Enabled'}")
print(f"  Debug mode: {'Enabled' if args.debug else 'Disabled'}")

# Initialize the SyncedDatabase unless disabled
synced_db = None
if not args.no_sync:
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

# Initialize database manager for query operations
db_manager = DatabaseManager(DB_PATH)

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
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
    return [table[0] for table in tables]

def get_table_schema(table_name):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
    return schema

def get_table_data(table_name):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name};")
        data = cursor.fetchall()
        schema = get_table_schema(table_name)
        columns = [col[1] for col in schema]
    return columns, data

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
    # Get all beers from the database
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM beers")
        beers = cursor.fetchall()
        
        # Get column names
        cursor.execute("PRAGMA table_info(beers)")
        columns = [info[1] for info in cursor.fetchall()]
    
    # Create a CSV string
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(columns)
    
    # Write data
    for beer in beers:
        writer.writerow(beer)
    
    # Prepare response
    csv_content = output.getvalue()
    output.close()
    
    response = app.response_class(
        response=csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=beers_backup.csv'}
    )
    
    return response

@app.route('/api/beers/import', methods=['POST'])
@login_required
def import_beers():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "File must be a CSV"}), 400
    
    # Read CSV file
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        reader = csv.DictReader(stream)
        
        imported_count = 0
        errors = []
        
        # Process each row in the CSV
        for row in reader:
            try:
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
                
                # Handle existing beer with same ID
                beer_id = row.get('idBeer')
                existing_beer = None
                
                if beer_id and beer_id.strip() and beer_id != '':
                    try:
                        beer_id = int(beer_id)
                        if synced_db:
                            existing_beer = synced_db.get_beer(beer_id)
                        else:
                            conn = sqlite3.connect(DB_PATH)
                            conn.row_factory = sqlite3.Row
                            cursor = conn.cursor()
                            cursor.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
                            existing_beer = cursor.fetchone()
                            conn.close()
                    except (ValueError, TypeError):
                        beer_id = None
                
                # If beer exists, update it; otherwise add new beer
                if existing_beer:
                    if synced_db:
                        synced_db.update_beer(
                            beer_id=beer_id,
                            name=row.get('Name'),
                            abv=row.get('ABV'),
                            ibu=row.get('IBU'),
                            color=row.get('Color'),
                            og=row.get('OriginalGravity'),
                            fg=row.get('FinalGravity'),
                            description=row.get('Description'),
                            brewed=row.get('Brewed'),
                            kegged=row.get('Kegged'),
                            tapped=row.get('Tapped'),
                            notes=row.get('Notes')
                        )
                    else:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute('''
                            UPDATE beers SET
                                Name = ?, ABV = ?, IBU = ?, Color = ?, OriginalGravity = ?, FinalGravity = ?,
                                Description = ?, Brewed = ?, Kegged = ?, Tapped = ?, Notes = ?
                            WHERE idBeer = ?
                        ''', (
                            row.get('Name'),
                            row.get('ABV'),
                            row.get('IBU'),
                            row.get('Color'),
                            row.get('OriginalGravity'),
                            row.get('FinalGravity'),
                            row.get('Description'),
                            row.get('Brewed'),
                            row.get('Kegged'),
                            row.get('Tapped'),
                            row.get('Notes'),
                            beer_id
                        ))
                        log_change(conn, "beers", "UPDATE", beer_id)
                        conn.commit()
                        conn.close()
                else:
                    # Add new beer
                    if synced_db:
                        synced_db.add_beer(
                            name=row.get('Name'),
                            abv=row.get('ABV'),
                            ibu=row.get('IBU'),
                            color=row.get('Color'),
                            og=row.get('OriginalGravity'),
                            fg=row.get('FinalGravity'),
                            description=row.get('Description'),
                            brewed=row.get('Brewed'),
                            kegged=row.get('Kegged'),
                            tapped=row.get('Tapped'),
                            notes=row.get('Notes')
                        )
                    else:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO beers (
                                Name, ABV, IBU, Color, OriginalGravity, FinalGravity,
                                Description, Brewed, Kegged, Tapped, Notes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            row.get('Name'),
                            row.get('ABV'),
                            row.get('IBU'),
                            row.get('Color'),
                            row.get('OriginalGravity'),
                            row.get('FinalGravity'),
                            row.get('Description'),
                            row.get('Brewed'),
                            row.get('Kegged'),
                            row.get('Tapped'),
                            row.get('Notes')
                        ))
                        beer_id = cursor.lastrowid
                        log_change(conn, "beers", "INSERT", beer_id)
                        conn.commit()
                        conn.close()
                
                imported_count += 1
                
            except Exception as e:
                errors.append(f"Error on row {reader.line_num}: {str(e)}")
        
        result = {
            "success": True,
            "imported_count": imported_count
        }
        
        if errors:
            result["errors"] = errors
            
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": f"Error processing CSV: {str(e)}"}), 500

@app.route('/api/beers/clear', methods=['POST'])
@login_required
def clear_beers():
    # Check for confirmation
    confirmation = request.json.get('confirmation')
    if not confirmation or confirmation != 'CONFIRM':
        return jsonify({"error": "Confirmation required"}), 400
    
    try:
        # Get count of beers before clearing
        if synced_db:
            beers = synced_db.get_all_beers()
            beer_count = len(beers)
            
            # Clear all taps first
            taps = synced_db.get_all_taps()
            for tap in taps:
                if tap['idBeer']:
                    synced_db.update_tap(tap['idTap'], None)
            
            # Delete each beer individually to trigger proper sync events
            for beer in beers:
                synced_db.delete_beer(beer['idBeer'])
        else:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Get beer count
            cursor.execute("SELECT COUNT(*) FROM beers")
            beer_count = cursor.fetchone()[0]
            
            # Clear all tap assignments first
            cursor.execute("UPDATE taps SET idBeer = NULL")
            
            # Get all beer IDs for logging
            cursor.execute("SELECT idBeer FROM beers")
            beer_ids = [row[0] for row in cursor.fetchall()]
            
            # Delete all beers
            cursor.execute("DELETE FROM beers")
            
            # Log each deletion
            for beer_id in beer_ids:
                log_change(conn, "beers", "DELETE", beer_id)
            
            conn.commit()
            conn.close()
        
        return jsonify({
            "success": True,
            "message": f"Successfully cleared {beer_count} beers from database"
        })
    
    except Exception as e:
        return jsonify({"error": f"Error clearing beers: {str(e)}"}), 500

# This function is now used by the web interface to log changes
# The SyncedDatabase handles the synchronization
def log_change(conn, table_name, operation, row_id):
    """Log a database change and trigger synchronization"""
    cursor = conn.cursor()
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Calculate content hash for the table
    cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (row_id,))
    row = cursor.fetchone()
    if row:
        content_hash = str(row)
    else:
        content_hash = "0"
    
    cursor.execute('''
        INSERT INTO change_log (table_name, operation, row_id, timestamp, content_hash)
        VALUES (?, ?, ?, ?, ?)
    ''', (table_name, operation, row_id, timestamp, content_hash))
    
    # Update version timestamp
    cursor.execute("UPDATE version SET last_modified = ?", (timestamp,))
    conn.commit()
    
    # Notify peers about the change if sync is enabled
    if synced_db:
        logger = logging.getLogger("KegDisplay")
        
        if table_name == 'beers':
            logger.info(f"Database change in 'beers': {operation} on row {row_id}")
            synced_db.notify_update()
        
        elif table_name == 'taps':
            logger.info(f"Database change in 'taps': {operation} on row {row_id}")
            synced_db.notify_update()

# --- API Endpoints ---

@app.route('/api/taps', methods=['GET'])
@login_required
def api_get_taps():
    """Get all taps with beer information"""
    try:
        with db_manager.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get all taps with beer information
            cursor.execute("""
                SELECT t.idTap, t.idBeer, 
                       b.Name as BeerName, b.ABV, b.IBU, b.Description 
                FROM taps t
                LEFT JOIN beers b ON t.idBeer = b.idBeer
                ORDER BY t.idTap
            """)
            
            taps = [dict(row) for row in cursor.fetchall()]
            
        return jsonify(taps)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['GET'])
@login_required
def api_get_tap(tap_id):
    if synced_db:
        tap = synced_db.get_tap(tap_id)
        
        if tap and tap['idBeer']:
            beer = synced_db.get_beer(tap['idBeer'])
            if beer:
                tap['BeerName'] = beer['Name']
                tap['ABV'] = beer['ABV']
                
        if tap:
            return jsonify(tap)
        else:
            return jsonify({"error": "Tap not found"}), 404
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT t.*, b.Name as BeerName, b.ABV 
            FROM taps t
            LEFT JOIN beers b ON t.idBeer = b.idBeer
            WHERE t.idTap = ?
        ''', (tap_id,))
        
        tap = cursor.fetchone()
        conn.close()
        
        if tap:
            return jsonify(dict(tap))
        else:
            return jsonify({"error": "Tap not found"}), 404

@app.route('/api/taps', methods=['POST'])
@login_required
def api_add_tap():
    """Add a new tap"""
    try:
        data = request.json
        
        # Get beer_id if provided, otherwise use NULL
        beer_id = data.get('idBeer')
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Find the next available tap ID
            cursor.execute("SELECT MAX(idTap) FROM taps")
            result = cursor.fetchone()
            next_tap_id = 1 if result[0] is None else result[0] + 1
            
            # Insert the new tap
            cursor.execute(
                "INSERT INTO taps (idTap, idBeer) VALUES (?, ?)",
                (next_tap_id, beer_id)
            )
            
            conn.commit()
            
            # Get the new tap with beer info if applicable
            if beer_id:
                cursor.execute("""
                    SELECT t.idTap, t.idBeer, 
                           b.Name as BeerName, b.ABV, b.IBU, b.Description 
                    FROM taps t
                    LEFT JOIN beers b ON t.idBeer = b.idBeer
                    WHERE t.idTap = ?
                """, (next_tap_id,))
            else:
                cursor.execute("SELECT idTap, idBeer FROM taps WHERE idTap = ?", (next_tap_id,))
            
            tap = dict(cursor.fetchone())
        
        # Notify peers of the update if using synced database
        if synced_db:
            synced_db.notify_update()
            
        return jsonify(tap), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['PUT'])
@login_required
def api_update_tap(tap_id):
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    beer_id = data.get('beer_id')
    
    if synced_db:
        # Check if tap exists
        tap = synced_db.get_tap(tap_id)
        if not tap:
            return jsonify({"error": f"Tap #{tap_id} not found"}), 404
            
        # Update the beer assignment
        if synced_db.update_tap(tap_id, beer_id):
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update tap"}), 500
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if tap exists
        cursor.execute("SELECT idTap FROM taps WHERE idTap = ?", (tap_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": f"Tap #{tap_id} not found"}), 404
        
        # Update the beer assignment
        cursor.execute(
            "UPDATE taps SET idBeer = ? WHERE idTap = ?",
            (beer_id, tap_id)
        )
        log_change(conn, "taps", "UPDATE", tap_id)
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True})

@app.route('/api/taps/<int:tap_id>', methods=['DELETE'])
@login_required
def api_delete_tap(tap_id):
    if synced_db:
        # Check if tap exists
        tap = synced_db.get_tap(tap_id)
        if not tap:
            return jsonify({"error": f"Tap #{tap_id} not found"}), 404
            
        # Delete the tap
        if synced_db.delete_tap(tap_id):
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to delete tap"}), 500
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if tap exists
        cursor.execute("SELECT idTap FROM taps WHERE idTap = ?", (tap_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": f"Tap #{tap_id} not found"}), 404
        
        # Delete the tap
        cursor.execute("DELETE FROM taps WHERE idTap = ?", (tap_id,))
        
        # Log the change
        log_change(conn, "taps", "DELETE", tap_id)
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True})

@app.route('/api/beers', methods=['GET'])
@login_required
def api_get_beers():
    if synced_db:
        beers = synced_db.get_all_beers()
        return jsonify(beers)
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM beers ORDER BY Name")
        
        beers = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify(beers)

@app.route('/api/beers/<int:beer_id>', methods=['GET'])
@login_required
def api_get_beer(beer_id):
    if synced_db:
        beer = synced_db.get_beer(beer_id)
        
        if beer:
            return jsonify(beer)
        else:
            return jsonify({"error": "Beer not found"}), 404
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
        
        beer = cursor.fetchone()
        conn.close()
        
        if beer:
            return jsonify(dict(beer))
        else:
            return jsonify({"error": "Beer not found"}), 404

@app.route('/api/beers/<int:beer_id>/taps', methods=['GET'])
@login_required
def api_get_beer_taps(beer_id):
    if synced_db:
        taps = synced_db.get_tap_with_beer(beer_id)
        return jsonify(taps)
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT idTap FROM taps WHERE idBeer = ? ORDER BY idTap", (beer_id,))
        
        taps = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return jsonify(taps)

@app.route('/api/beers', methods=['POST'])
@login_required
def api_add_beer():
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    name = data.get('Name')
    
    # Validate data
    if not name:
        return jsonify({"error": "Beer name is required"}), 400
    
    if synced_db:
        # Add the beer
        beer_data = {
            'name': name,
            'abv': data.get('ABV'),
            'ibu': data.get('IBU'),
            'color': data.get('Color'),
            'og': data.get('OriginalGravity'),  # Use the correct field from frontend
            'fg': data.get('FinalGravity'),     # Use the correct field from frontend
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
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Add the beer
        cursor.execute('''
            INSERT INTO beers (
                Name, ABV, IBU, Color, OriginalGravity, FinalGravity,
                Description, Brewed, Kegged, Tapped, Notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            name,
            data.get('ABV'),
            data.get('IBU'),
            data.get('Color'),
            data.get('OriginalGravity'),
            data.get('FinalGravity'),
            data.get('Description'),
            data.get('Brewed'),
            data.get('Kegged'),
            data.get('Tapped'),
            data.get('Notes')
        ))
        
        beer_id = cursor.lastrowid
        
        # Log the change
        log_change(conn, "beers", "INSERT", beer_id)
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "beer_id": beer_id}), 201

@app.route('/api/beers/<int:beer_id>', methods=['PUT'])
@login_required
def api_update_beer(beer_id):
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    name = data.get('Name')
    
    # Validate data
    if not name:
        return jsonify({"error": "Beer name is required"}), 400
    
    if synced_db:
        # Check if beer exists
        beer = synced_db.get_beer(beer_id)
        if not beer:
            return jsonify({"error": "Beer not found"}), 404
            
        # Update the beer
        beer_data = {
            'beer_id': beer_id,
            'name': name,
            'abv': data.get('ABV'),
            'ibu': data.get('IBU'),
            'color': data.get('Color'),
            'og': data.get('OriginalGravity'),  # Use the correct field from frontend
            'fg': data.get('FinalGravity'),     # Use the correct field from frontend
            'description': data.get('Description'),
            'brewed': data.get('Brewed'),
            'kegged': data.get('Kegged'),
            'tapped': data.get('Tapped'),
            'notes': data.get('Notes')
        }
        
        if synced_db.update_beer(**beer_data):
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update beer"}), 500
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if beer exists
        cursor.execute("SELECT idBeer FROM beers WHERE idBeer = ?", (beer_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "Beer not found"}), 404
        
        # Update the beer
        cursor.execute('''
            UPDATE beers SET
                Name = ?, ABV = ?, IBU = ?, Color = ?, OriginalGravity = ?, FinalGravity = ?,
                Description = ?, Brewed = ?, Kegged = ?, Tapped = ?, Notes = ?
            WHERE idBeer = ?
        ''', (
            name,
            data.get('ABV'),
            data.get('IBU'),
            data.get('Color'),
            data.get('OriginalGravity'),
            data.get('FinalGravity'),
            data.get('Description'),
            data.get('Brewed'),
            data.get('Kegged'),
            data.get('Tapped'),
            data.get('Notes'),
            beer_id
        ))
        
        # Log the change
        log_change(conn, "beers", "UPDATE", beer_id)
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True})

@app.route('/api/beers/<int:beer_id>', methods=['DELETE'])
@login_required
def api_delete_beer(beer_id):
    if synced_db:
        # Check if beer exists
        beer = synced_db.get_beer(beer_id)
        if not beer:
            return jsonify({"error": "Beer not found"}), 404
            
        # Delete the beer (this will also update any taps using this beer)
        if synced_db.delete_beer(beer_id):
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to delete beer"}), 500
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if beer exists
        cursor.execute("SELECT idBeer FROM beers WHERE idBeer = ?", (beer_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "Beer not found"}), 404
        
        # Check if any taps are using this beer
        cursor.execute("SELECT idTap FROM taps WHERE idBeer = ?", (beer_id,))
        affected_taps = [row[0] for row in cursor.fetchall()]
        
        if affected_taps:
            # Update those taps to remove the beer
            for tap_id in affected_taps:
                cursor.execute("UPDATE taps SET idBeer = NULL WHERE idTap = ?", (tap_id,))
                log_change(conn, "taps", "UPDATE", tap_id)
        
        # Delete the beer
        cursor.execute("DELETE FROM beers WHERE idBeer = ?", (beer_id,))
        
        # Log the change
        log_change(conn, "beers", "DELETE", beer_id)
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True})

@app.route('/api/taps/count', methods=['POST'])
@login_required
def api_set_tap_count():
    data = request.json
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    count = data.get('count')
    
    if count is None or not isinstance(count, int) or count < 1:
        return jsonify({"error": "Valid tap count is required (must be positive integer)"}), 400
    
    if synced_db:
        # Get current taps
        existing_taps = synced_db.get_all_taps()
        current_count = len(existing_taps)
        
        # If decreasing, delete excess taps
        if count < current_count:
            # Delete taps from highest number to lowest
            for i in range(current_count, count, -1):
                tap_id = i
                synced_db.delete_tap(tap_id)
        
        # If increasing, add new taps
        elif count > current_count:
            # Add new taps with sequential IDs
            for i in range(current_count + 1, count + 1):
                tap_id = i
                synced_db.add_tap(tap_id, None)
        
        return jsonify({"success": True, "tap_count": count})
    else:
        # Fallback if synced_db is not available
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get current tap count
        cursor.execute("SELECT COUNT(*) FROM taps")
        current_count = cursor.fetchone()[0]
        
        # If decreasing, delete excess taps
        if count < current_count:
            # Delete taps from highest number to lowest
            for i in range(current_count, count, -1):
                cursor.execute("DELETE FROM taps WHERE idTap = ?", (i,))
                log_change(conn, "taps", "DELETE", i)
        
        # If increasing, add new taps
        elif count > current_count:
            # Add new taps with sequential IDs
            for i in range(current_count + 1, count + 1):
                cursor.execute("INSERT INTO taps (idTap, idBeer) VALUES (?, NULL)", (i,))
                log_change(conn, "taps", "INSERT", i)
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "tap_count": count})

def start(passed_args=None):
    """
    Main entry point for the web interface.
    
    Args:
        passed_args: Pre-parsed arguments (optional)
    """
    # Use passed arguments or parse again
    args = passed_args or parse_args()
    
    # Configure logging with the specified level
    configure_logging(args.log_level)
    logger.debug(f"Starting KegDisplay web interface with log level {args.log_level}")
    
    # Initialize database synchronization if enabled
    if not args.no_sync and synced_db:
        # The SyncedDatabase already starts the synchronizer in its constructor
        logger.info(f"Database synchronization active on ports {args.broadcast_port} (UDP) and {args.sync_port} (TCP)")
    
    # Import Gunicorn here to avoid importing it when not needed
    try:
        from gunicorn.app.base import BaseApplication
    except ImportError:
        logger.error("Gunicorn is not installed. Please install it with: pip install gunicorn")
        sys.exit(1)
    
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
    
    # Configure Gunicorn options optimized for Raspberry Pi Zero 2W
    options = {
        'bind': f"{args.host}:{args.port}",
        'workers': args.workers,
        'worker_class': args.worker_class,
        'timeout': args.timeout,
        'worker_connections': 100,  # Conservative limit for Pi Zero 2W
        'max_requests': 1000,  # Restart workers periodically to prevent memory leaks
        'max_requests_jitter': 50,  # Add jitter to prevent all workers restarting at once
        'keepalive': 2,  # Short keepalive timeout to free resources quickly
        'graceful_timeout': 30,  # Time to wait for workers to finish their requests
        'accesslog': '-',  # Log to stdout
        'errorlog': '-',   # Log to stderr
        'loglevel': args.log_level.lower(),
        'capture_output': True,
        'enable_stdio_inheritance': True,
        'daemon': False,  # Run in foreground for systemd
        'pidfile': None,  # Don't create pidfile (systemd handles this)
        'umask': 0,  # Default umask
        'user': None,  # Let systemd handle user
        'group': None,  # Let systemd handle group
        'tmp_upload_dir': None,  # Use system default
        'reload': args.debug,  # Enable auto-reload in debug mode
    }
    
    # Add SSL configuration if certificates are provided
    if args.ssl_cert and args.ssl_key:
        if not os.path.exists(args.ssl_cert):
            logger.error(f"SSL certificate file not found: {args.ssl_cert}")
            sys.exit(1)
        if not os.path.exists(args.ssl_key):
            logger.error(f"SSL private key file not found: {args.ssl_key}")
            sys.exit(1)
        options['certfile'] = args.ssl_cert
        options['keyfile'] = args.ssl_key
        logger.info(f"SSL enabled with certificate: {args.ssl_cert}")
    
    # Start the Gunicorn server
    logger.info(f"Starting Gunicorn server on {args.host}:{args.port}")
    logger.info(f"Worker configuration: {args.workers} workers, {args.worker_class} worker class")
    KegDisplayApplication(app, options).run()

if __name__ == '__main__':
    start() 
