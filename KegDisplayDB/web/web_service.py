import os
import argparse
import logging
import bcrypt
import sys
import time
import subprocess
import requests
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from .db_client import DbClient
from ..utils.log_config import configure_logging

# Try to import gunicorn's BaseApplication
try:
    from gunicorn.app.base import BaseApplication
except ImportError:
    BaseApplication = None

# Setup Flask app
BASE_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = os.urandom(24)
logger = logging.getLogger("KegDisplayDB.web_service")

# Setup login manager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Default RPC endpoint for Sync service
default_rpc_url = os.getenv('KEGDISPLAY_RPC_URL', 'http://127.0.0.1:5001')
db_client = None
sync_service_running = False

# Define paths and directories
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
DEFAULT_SSL_CERT = os.path.join(SSL_DIR, 'certs', 'kegdisplay.crt')
DEFAULT_SSL_KEY = os.path.join(SSL_DIR, 'private', 'kegdisplay.key')

# User model and loader
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
    etc_dir = os.path.join(os.path.expanduser('~'), '.KegDisplayDB', 'etc')
    passwd_path = os.getenv('KEGDISPLAY_PASSWD_PATH', os.path.join(etc_dir, 'passwd'))
    try:
        with open(passwd_path, 'r') as f:
            for line in f:
                username, password_hash = line.strip().split(':')
                users[username] = password_hash
    except FileNotFoundError:
        logger.warning(f"Password file not found at {passwd_path}")
    return users

@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)

# Web routes
@app.route('/')
@login_required
def index():
    return redirect(url_for('taps'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        users = load_users()
        if username in users and bcrypt.checkpw(password.encode('utf-8'), users[username].encode('utf-8')):
            user = User(username, users[username])
            login_user(user)
            return redirect(url_for('index'))
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
    taps = db_client.get_taps()
    return render_template('taps.html', taps=taps, active_page='taps')

@app.route('/beers')
@login_required
def beers():
    beers = db_client.get_beers()
    return render_template('beers.html', beers=beers, active_page='beers')

@app.route('/dbmanage')
@login_required
def db_manage():
    return render_template('dbmanage.html', active_page='dbmanage')

# API routes mirroring sync_service RPC endpoints
@app.route('/api/beers', methods=['GET'])
@login_required
def api_get_beers():
    try:
        return jsonify(db_client.get_beers())
    except Exception as e:
        logger.error(f"Error fetching beers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['GET'])
@login_required
def api_get_beer(beer_id):
    try:
        beer = db_client.get_beer(beer_id)
        if not beer:
            return jsonify({"error": "Beer not found"}), 404
        return jsonify(beer)
    except Exception as e:
        logger.error(f"Error fetching beer {beer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers', methods=['POST'])
@login_required
def api_add_beer():
    try:
        data = request.json or {}
        beer_id = db_client.add_beer(data)
        return jsonify({"success": True, "beer_id": beer_id}), 201
    except Exception as e:
        logger.error(f"Error adding beer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['PUT'])
@login_required
def api_update_beer(beer_id):
    try:
        data = request.json or {}
        success = db_client.update_beer(beer_id, data)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to update beer"}), 500        
    except Exception as e:
        logger.error(f"Error updating beer {beer_id}: {e}")     
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/<int:beer_id>', methods=['DELETE'])
@login_required
def api_delete_beer(beer_id):
    try:
        success = db_client.delete_beer(beer_id)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to delete beer"}), 500
    except Exception as e:
        logger.error(f"Error deleting beer {beer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps', methods=['GET'])
@login_required
def api_get_taps():
    try:
        return jsonify(db_client.get_taps())
    except Exception as e:
        logger.error(f"Error fetching taps: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['GET'])
@login_required
def api_get_tap(tap_id):
    try:
        tap = db_client.get_tap(tap_id)
        if not tap:
            return jsonify({"error": "Tap not found"}), 404
        return jsonify(tap)
    except Exception as e:
        logger.error(f"Error fetching tap {tap_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps', methods=['POST'])
@login_required
def api_add_tap():
    try:
        data = request.json or {}
        tap = db_client.add_tap(data)
        return jsonify(tap), 201
    except Exception as e:
        logger.error(f"Error adding tap: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['PUT'])
@login_required
def api_update_tap(tap_id):
    try:
        data = request.json or {}
        success = db_client.update_tap(tap_id, data)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to update tap"}), 500
    except Exception as e:
        logger.error(f"Error updating tap {tap_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/<int:tap_id>', methods=['DELETE'])
@login_required
def api_delete_tap(tap_id):
    try:
        success = db_client.delete_tap(tap_id)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to delete tap"}), 500
    except Exception as e:
        logger.error(f"Error deleting tap {tap_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/count', methods=['POST'])
@login_required
def api_set_tap_count():
    try:
        data = request.json or {}
        count = data.get('count')
        result = db_client.set_tap_count(count)
        return jsonify({"success": True, "tap_count": result})
    except Exception as e:
        logger.error(f"Error setting tap count: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/backup', methods=['GET'])
@login_required
def api_backup_beers():
    try:
        csv_data = db_client.backup_beers()
        return csv_data, 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=beers_backup.csv'}
    except Exception as e:
        logger.error(f"Error backing up beers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/clear', methods=['POST'])
@login_required
def api_clear_beers():
    try:
        result = db_client.clear_beers()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error clearing beers: {e}")
        return jsonify({"error": str(e)}), 500

# CSV import endpoints
@app.route('/api/beers/import', methods=['POST'])
@login_required
def api_import_beers():
    # Proxy CSV upload to sync service
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files['file']
    resp, status = db_client.import_beers(file)
    return jsonify(resp), status

@app.route('/api/beers/import-status', methods=['GET'])
@login_required
def api_import_status():
    try:
        status = db_client.get_import_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error fetching import status: {e}")
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

def check_sync_service():
    """Check if the sync service is running and accessible"""
    global sync_service_running
    try:
        response = requests.get(f"{default_rpc_url}/rpc/beers", timeout=2)
        sync_service_running = response.status_code == 200
        return sync_service_running
    except requests.exceptions.RequestException:
        sync_service_running = False
        return False

@app.before_request
def check_backend():
    """Check if sync service is running before each request"""
    if request.endpoint and request.endpoint != 'login':
        if not check_sync_service():
            logger.warning("Sync service is not available")
            if request.endpoint.startswith('api_'):
                return jsonify({"error": "Database sync service is not available. Talk to your administrator to resolve this issue."}), 503
            else:
                flash("Database sync service is not available. Talk to your administrator to resolve this issue.")
                return redirect(url_for('login'))

class KegDisplayApplication(BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()
    
    def load_config(self):
        config = {key: value for key, value in self.options.items()
                 if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)
    
    def load(self):
        return self.application

def parse_args():
    parser = argparse.ArgumentParser(description="Web service API")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--rpc-url", default=default_rpc_url, help="Sync RPC service URL")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Set logging level")
    parser.add_argument("--ssl-cert", type=str, default=DEFAULT_SSL_CERT, help="Path to SSL certificate file")
    parser.add_argument("--ssl-key", type=str, default=DEFAULT_SSL_KEY, help="Path to SSL private key file")
    parser.add_argument("--use-ssl", action="store_true", default=False, help="Enable SSL/TLS")
    parser.add_argument("--workers", type=int, default=2, help="Number of Gunicorn worker processes")
    parser.add_argument("--timeout", type=int, default=30, help="Worker timeout in seconds")
    return parser.parse_args()

def main(use_ssl=None, port=None):
    global db_client
    args = parse_args()
    
    # Override arguments if provided directly
    if use_ssl is not None:
        args.use_ssl = use_ssl
    if port is not None:
        args.port = port
    
    configure_logging(log_level=args.log_level)
    logger.setLevel(getattr(logging, args.log_level))
    
    # Initialize db_client but don't require sync service to be running
    db_client = DbClient(args.rpc_url)
    
    # Check if Gunicorn is available
    if BaseApplication is None:
        logger.error("Gunicorn is not installed. Please install it with: pip install gunicorn")
        sys.exit(1)

    # Display configuration
    logger.info(f"Web service configuration:")
    logger.info(f"  Host: {args.host}")
    logger.info(f"  Web port: {args.port}")
    logger.info(f"  RPC URL: {args.rpc_url}")
    logger.info(f"  SSL: {'Enabled' if args.use_ssl else 'Disabled'}")
    logger.info(f"  Debug mode: {'Enabled' if args.debug else 'Disabled'}")
    
    # Check if sync service is available
    if check_sync_service():
        logger.info("Sync service is available")
    else:
        logger.warning("Sync service is not available. Please start it manually.")

    # Configure Gunicorn options
    options = {
        'bind': f"{args.host}:{args.port}",
        'workers': args.workers,
        'worker_class': 'gthread',
        'threads': args.workers,
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

    # Add SSL configuration if enabled
    if args.use_ssl:
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
    logger.info(f"Worker configuration: {args.workers} workers")
    KegDisplayApplication(app, options).run()

def main_ssl():
    """Start the web service with SSL on port 8443"""
    main(use_ssl=True, port=8443)

def main_nossl():
    """Start the web service without SSL on port 8080"""
    main(use_ssl=False, port=8080)

if __name__ == "__main__":
    main() 