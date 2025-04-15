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
DB_PATH = os.path.join(BASE_DIR, '..', '..', 'beer.db')
PASSWD_PATH = os.path.join(BASE_DIR, '..', '..', 'passwd')
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
print(f"Password file path: {PASSWD_PATH}")
print(f"Template directory: {TEMPLATE_DIR}")

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
                       default=os.path.join(BASE_DIR, '..', '..', 'ssl', 'kegdisplay.crt'),
                       help='Path to SSL certificate file (default: development certificate)')
    parser.add_argument('--ssl-key', type=str,
                       default=os.path.join(BASE_DIR, '..', '..', 'ssl', 'kegdisplay.key'),
                       help='Path to SSL private key file (default: development certificate)')
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
        headers={'Content-Disposition': 'attachment;filename=beers_backup.csv'}
    )
    return response

@app.route('/api/beers/import', methods=['POST'])
@login_required
def import_beers():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400
    
    try:
        # Read CSV file
        csv_data = file.read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(csv_data))
        
        # Get column names from CSV
        csv_columns = csv_reader.fieldnames
        
        # Get database schema
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(beers)")
            db_columns = [info[1] for info in cursor.fetchall()]
        
        # Check if all required columns are present
        missing_columns = set(db_columns) - set(csv_columns)
        if missing_columns:
            return jsonify({
                'error': f'Missing required columns: {", ".join(missing_columns)}'
            }), 400
        
        # Import data
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            for row in csv_reader:
                # Create placeholders for the SQL query
                placeholders = ', '.join(['?' for _ in db_columns])
                columns = ', '.join(db_columns)
                
                # Get values in the correct order
                values = [row.get(col, None) for col in db_columns]
                
                # Insert the row
                cursor.execute(
                    f"INSERT INTO beers ({columns}) VALUES ({placeholders})",
                    values
                )
            
            conn.commit()
        
        return jsonify({'message': 'Import successful'}), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/beers/clear', methods=['POST'])
@login_required
def clear_beers():
    # Check for confirmation
    if not request.json or 'confirm' not in request.json or not request.json['confirm']:
        return jsonify({'error': 'Confirmation required'}), 400
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            # First, clear any references in the taps table
            cursor.execute("UPDATE taps SET beer_id = NULL")
            # Then clear the beers table
            cursor.execute("DELETE FROM beers")
            conn.commit()
        
        return jsonify({'message': 'Database cleared successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def log_change(conn, table_name, operation, row_id):
    """Log a change to the database for synchronization"""
    cursor = conn.cursor()
    timestamp = datetime.now(UTC).isoformat()
    cursor.execute(
        "INSERT INTO change_log (table_name, operation, row_id, timestamp) VALUES (?, ?, ?, ?)",
        (table_name, operation, row_id, timestamp)
    )

@app.route('/api/taps', methods=['GET'])
@login_required
def api_get_taps():
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT t.*, b.name as beer_name, b.style as beer_style, b.abv as beer_abv
            FROM taps t
            LEFT JOIN beers b ON t.beer_id = b.id
            ORDER BY t.id
        """)
        taps = cursor.fetchall()
        
        # Get column names
        cursor.execute("PRAGMA table_info(taps)")
        columns = [info[1] for info in cursor.fetchall()]
        columns.extend(['beer_name', 'beer_style', 'beer_abv'])
    
    return jsonify([dict(zip(columns, tap)) for tap in taps])

@app.route('/api/taps/<int:tap_id>', methods=['GET'])
@login_required
def api_get_tap(tap_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT t.*, b.name as beer_name, b.style as beer_style, b.abv as beer_abv
            FROM taps t
            LEFT JOIN beers b ON t.beer_id = b.id
            WHERE t.id = ?
        """, (tap_id,))
        tap = cursor.fetchone()
        
        if tap is None:
            return jsonify({'error': 'Tap not found'}), 404
        
        # Get column names
        cursor.execute("PRAGMA table_info(taps)")
        columns = [info[1] for info in cursor.fetchall()]
        columns.extend(['beer_name', 'beer_style', 'beer_abv'])
    
    return jsonify(dict(zip(columns, tap)))

@app.route('/api/taps', methods=['POST'])
@login_required
def api_add_tap():
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    required_fields = ['name', 'position']
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return jsonify({'error': f'Missing required fields: {", ".join(missing_fields)}'}), 400
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO taps (name, position, beer_id, is_active)
                VALUES (?, ?, ?, ?)
            """, (
                data['name'],
                data['position'],
                data.get('beer_id'),
                data.get('is_active', True)
            ))
            tap_id = cursor.lastrowid
            log_change(conn, 'taps', 'INSERT', tap_id)
            conn.commit()
        
        return jsonify({'message': 'Tap added successfully', 'id': tap_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['PUT'])
@login_required
def api_update_tap(tap_id):
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if tap exists
            cursor.execute("SELECT id FROM taps WHERE id = ?", (tap_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Tap not found'}), 404
            
            # Build update query
            update_fields = []
            values = []
            for field in ['name', 'position', 'beer_id', 'is_active']:
                if field in data:
                    update_fields.append(f"{field} = ?")
                    values.append(data[field])
            
            if not update_fields:
                return jsonify({'error': 'No fields to update'}), 400
            
            values.append(tap_id)
            query = f"""
                UPDATE taps
                SET {', '.join(update_fields)}
                WHERE id = ?
            """
            
            cursor.execute(query, values)
            log_change(conn, 'taps', 'UPDATE', tap_id)
            conn.commit()
        
        return jsonify({'message': 'Tap updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['DELETE'])
@login_required
def api_delete_tap(tap_id):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if tap exists
            cursor.execute("SELECT id FROM taps WHERE id = ?", (tap_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Tap not found'}), 404
            
            cursor.execute("DELETE FROM taps WHERE id = ?", (tap_id,))
            log_change(conn, 'taps', 'DELETE', tap_id)
            conn.commit()
        
        return jsonify({'message': 'Tap deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/beers', methods=['GET'])
@login_required
def api_get_beers():
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM beers ORDER BY name")
        beers = cursor.fetchall()
        
        # Get column names
        cursor.execute("PRAGMA table_info(beers)")
        columns = [info[1] for info in cursor.fetchall()]
    
    return jsonify([dict(zip(columns, beer)) for beer in beers])

@app.route('/api/beers/<int:beer_id>', methods=['GET'])
@login_required
def api_get_beer(beer_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM beers WHERE id = ?", (beer_id,))
        beer = cursor.fetchone()
        
        if beer is None:
            return jsonify({'error': 'Beer not found'}), 404
        
        # Get column names
        cursor.execute("PRAGMA table_info(beers)")
        columns = [info[1] for info in cursor.fetchall()]
    
    return jsonify(dict(zip(columns, beer)))

@app.route('/api/beers/<int:beer_id>/taps', methods=['GET'])
@login_required
def api_get_beer_taps(beer_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT t.*
            FROM taps t
            WHERE t.beer_id = ?
            ORDER BY t.position
        """, (beer_id,))
        taps = cursor.fetchall()
        
        # Get column names
        cursor.execute("PRAGMA table_info(taps)")
        columns = [info[1] for info in cursor.fetchall()]
    
    return jsonify([dict(zip(columns, tap)) for tap in taps])

@app.route('/api/beers', methods=['POST'])
@login_required
def api_add_beer():
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    required_fields = ['name', 'style', 'abv']
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return jsonify({'error': f'Missing required fields: {", ".join(missing_fields)}'}), 400
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO beers (name, style, abv, description, brewery, image_url)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                data['name'],
                data['style'],
                data['abv'],
                data.get('description'),
                data.get('brewery'),
                data.get('image_url')
            ))
            beer_id = cursor.lastrowid
            log_change(conn, 'beers', 'INSERT', beer_id)
            conn.commit()
        
        return jsonify({'message': 'Beer added successfully', 'id': beer_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['PUT'])
@login_required
def api_update_beer(beer_id):
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if beer exists
            cursor.execute("SELECT id FROM beers WHERE id = ?", (beer_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Beer not found'}), 404
            
            # Build update query
            update_fields = []
            values = []
            for field in ['name', 'style', 'abv', 'description', 'brewery', 'image_url']:
                if field in data:
                    update_fields.append(f"{field} = ?")
                    values.append(data[field])
            
            if not update_fields:
                return jsonify({'error': 'No fields to update'}), 400
            
            values.append(beer_id)
            query = f"""
                UPDATE beers
                SET {', '.join(update_fields)}
                WHERE id = ?
            """
            
            cursor.execute(query, values)
            log_change(conn, 'beers', 'UPDATE', beer_id)
            conn.commit()
        
        return jsonify({'message': 'Beer updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['DELETE'])
@login_required
def api_delete_beer(beer_id):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if beer exists
            cursor.execute("SELECT id FROM beers WHERE id = ?", (beer_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Beer not found'}), 404
            
            # First, clear any references in the taps table
            cursor.execute("UPDATE taps SET beer_id = NULL WHERE beer_id = ?", (beer_id,))
            
            # Then delete the beer
            cursor.execute("DELETE FROM beers WHERE id = ?", (beer_id,))
            log_change(conn, 'beers', 'DELETE', beer_id)
            conn.commit()
        
        return jsonify({'message': 'Beer deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/taps/count', methods=['POST'])
@login_required
def api_set_tap_count():
    data = request.json
    if not data or 'count' not in data:
        return jsonify({'error': 'No count provided'}), 400
    
    try:
        count = int(data['count'])
        if count < 0:
            return jsonify({'error': 'Count must be non-negative'}), 400
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get current tap count
            cursor.execute("SELECT COUNT(*) FROM taps")
            current_count = cursor.fetchone()[0]
            
            if count > current_count:
                # Add new taps
                for i in range(current_count + 1, count + 1):
                    cursor.execute("""
                        INSERT INTO taps (name, position, is_active)
                        VALUES (?, ?, ?)
                    """, (f"Tap {i}", i, True))
                    log_change(conn, 'taps', 'INSERT', cursor.lastrowid)
            elif count < current_count:
                # Remove excess taps
                cursor.execute("""
                    DELETE FROM taps
                    WHERE id > ?
                """, (count,))
                # Log the deletion of each tap
                for tap_id in range(count + 1, current_count + 1):
                    log_change(conn, 'taps', 'DELETE', tap_id)
            
            conn.commit()
        
        return jsonify({'message': f'Number of taps set to {count}'}), 200
    except ValueError:
        return jsonify({'error': 'Count must be an integer'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def start():
    """Start the web interface"""
    # Configure logging
    configure_logging()
    
    # Start the Flask application
    if args.debug:
        app.run(host=args.host, port=args.port, debug=True)
    else:
        from gunicorn.app.base import BaseApplication
        
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
        
        options = {
            'bind': f'{args.host}:{args.port}',
            'workers': args.workers,
            'worker_class': args.worker_class,
            'timeout': args.timeout,
            'certfile': args.ssl_cert,
            'keyfile': args.ssl_key
        }
        
        KegDisplayApplication(app, options).run()

if __name__ == '__main__':
    start()
