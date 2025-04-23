import os
import argparse
import logging
import bcrypt
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from .db_client import DbClient
from ..utils.log_config import configure_logging
from datetime import datetime, UTC

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

@app.route('/api/system/status', methods=['GET'])
@login_required
def api_system_status():
    try:
        # Get basic system information
        status = {
            "status": "ok",
            "timestamp": datetime.now(UTC).isoformat(),
            "services": {
                "web_service": "running",
                "sync_service": "connected" if db_client else "disconnected"
            },
            "database": {
                "beer_count": len(db_client.get_beers()),
                "tap_count": len(db_client.get_taps())
            }
        }
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/search', methods=['GET'])
@login_required
def api_search_beers():
    try:
        query = request.args.get('q', '').lower()
        beers = db_client.get_beers()
        
        # Filter beers based on search query
        filtered_beers = []
        for beer in beers:
            # Search in name and description
            if query in beer.get('Name', '').lower() or query in beer.get('Description', '').lower():
                filtered_beers.append(beer)
        
        return jsonify({
            "count": len(filtered_beers),
            "results": filtered_beers
        })
    except Exception as e:
        logger.error(f"Error searching beers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/status', methods=['GET'])
@login_required
def api_taps_status():
    try:
        taps = db_client.get_taps()
        beers = db_client.get_beers()
        
        # Create a map of beer_id to beer details
        beer_map = {beer['idBeer']: beer for beer in beers}
        
        # Enhance tap information with beer details
        enhanced_taps = []
        for tap in taps:
            tap_info = tap.copy()
            beer_id = tap.get('idBeer')
            if beer_id and beer_id in beer_map:
                tap_info['beer'] = beer_map[beer_id]
            else:
                tap_info['beer'] = None
            enhanced_taps.append(tap_info)
        
        return jsonify({
            "count": len(enhanced_taps),
            "taps": enhanced_taps
        })
    except Exception as e:
        logger.error(f"Error getting tap status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats', methods=['GET'])
@login_required
def api_stats():
    try:
        beers = db_client.get_beers()
        taps = db_client.get_taps()
        
        # Calculate basic statistics
        stats = {
            "beers": {
                "total": len(beers),
                "by_abv": {
                    "average": sum(float(b.get('ABV', 0)) for b in beers) / len(beers) if beers else 0,
                    "min": min(float(b.get('ABV', 0)) for b in beers) if beers else 0,
                    "max": max(float(b.get('ABV', 0)) for b in beers) if beers else 0
                }
            },
            "taps": {
                "total": len(taps),
                "occupied": sum(1 for t in taps if t.get('idBeer') is not None),
                "empty": sum(1 for t in taps if t.get('idBeer') is None)
            }
        }
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/id', methods=['GET'])
@login_required
def api_beer_by_id():
    try:
        beer_id = request.args.get('q')
        if beer_id == '*':
            beers = db_client.get_beers()
            return jsonify({"count": len(beers), "beers": beers})
        try:
            beer_id = int(beer_id)
            beer = db_client.get_beer(beer_id)
            if beer:
                return jsonify(beer)
            return jsonify({"error": "Beer not found"}), 404
        except ValueError:
            return jsonify({"error": "Invalid beer ID"}), 400
    except Exception as e:
        logger.error(f"Error getting beer by ID: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/name', methods=['GET'])
@login_required
def api_beer_by_name():
    try:
        query = request.args.get('q', '').lower()
        beers = db_client.get_beers()
        
        if query == '*':
            return jsonify({"count": len(beers), "beers": beers})
        
        filtered_beers = []
        for beer in beers:
            name = beer.get('Name', '').lower()
            if query.endswith('*'):
                if name.startswith(query[:-1]):
                    filtered_beers.append(beer)
            elif query.startswith('*'):
                if name.endswith(query[1:]):
                    filtered_beers.append(beer)
            elif query in name:
                filtered_beers.append(beer)
        
        return jsonify({"count": len(filtered_beers), "beers": filtered_beers})
    except Exception as e:
        logger.error(f"Error searching beers by name: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/search', methods=['GET'])
@login_required
def api_beer_search():
    try:
        query = request.args.get('q', '').lower()
        beers = db_client.get_beers()
        
        if query == '*':
            return jsonify({"count": len(beers), "beers": beers})
        
        filtered_beers = []
        for beer in beers:
            # Search in all string fields
            for value in beer.values():
                if isinstance(value, (str, int, float)):
                    if query in str(value).lower():
                        filtered_beers.append(beer)
                        break
        
        return jsonify({"count": len(filtered_beers), "beers": filtered_beers})
    except Exception as e:
        logger.error(f"Error searching beers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/len', methods=['GET'])
@login_required
def api_beer_count():
    try:
        beers = db_client.get_beers()
        return jsonify({"count": len(beers)})
    except Exception as e:
        logger.error(f"Error getting beer count: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/last', methods=['GET'])
@login_required
def api_last_beers():
    try:
        count = request.args.get('q', '1')
        try:
            count = int(count)
            beers = db_client.get_beers()
            # Sort by idBeer in descending order and take the last 'count' records
            sorted_beers = sorted(beers, key=lambda x: x.get('idBeer', 0), reverse=True)
            return jsonify({"count": min(count, len(sorted_beers)), "beers": sorted_beers[:count]})
        except ValueError:
            return jsonify({"error": "Invalid count parameter"}), 400
    except Exception as e:
        logger.error(f"Error getting last beers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/beers/tap', methods=['GET'])
@login_required
def api_beer_tap():
    try:
        beer_id = request.args.get('q')
        try:
            beer_id = int(beer_id)
            taps = db_client.get_taps()
            for tap in taps:
                if tap.get('idBeer') == beer_id:
                    return jsonify({"tap_id": tap.get('idTap')})
            return jsonify({"tap_id": None})
        except ValueError:
            return jsonify({"error": "Invalid beer ID"}), 400
    except Exception as e:
        logger.error(f"Error finding beer tap: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/clock/current', methods=['GET'])
@login_required
def api_current_clock():
    try:
        # Assuming the clock value is stored in the database
        # You'll need to implement the actual clock retrieval logic
        return jsonify({"clock": 0})  # Placeholder
    except Exception as e:
        logger.error(f"Error getting current clock: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/version', methods=['GET'])
@login_required
def api_version():
    try:
        # Assuming version information is stored in the database
        # You'll need to implement the actual version retrieval logic
        return jsonify({"version": "1.0.0"})  # Placeholder
    except Exception as e:
        logger.error(f"Error getting version: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/id', methods=['GET'])
@login_required
def api_tap_beer():
    try:
        tap_id = request.args.get('q')
        try:
            tap_id = int(tap_id)
            tap = db_client.get_tap(tap_id)
            if tap:
                return jsonify({"beer_id": tap.get('idBeer')})
            return jsonify({"error": "Tap not found"}), 404
        except ValueError:
            return jsonify({"error": "Invalid tap ID"}), 400
    except Exception as e:
        logger.error(f"Error getting tap beer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/taps/beer', methods=['GET'])
@login_required
def api_beer_tap_number():
    try:
        beer_id = request.args.get('q')
        try:
            beer_id = int(beer_id)
            taps = db_client.get_taps()
            for tap in taps:
                if tap.get('idBeer') == beer_id:
                    return jsonify({"tap_number": tap.get('idTap')})
            return jsonify({"tap_number": None})
        except ValueError:
            return jsonify({"error": "Invalid beer ID"}), 400
    except Exception as e:
        logger.error(f"Error finding beer tap number: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/change/last', methods=['GET'])
@login_required
def api_last_changes():
    try:
        count = request.args.get('q', '1')
        try:
            count = int(count)
            # You'll need to implement the actual change record retrieval logic
            return jsonify({"count": 0, "changes": []})  # Placeholder
        except ValueError:
            return jsonify({"error": "Invalid count parameter"}), 400
    except Exception as e:
        logger.error(f"Error getting last changes: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/change/since', methods=['GET'])
@login_required
def api_changes_since():
    try:
        clock = request.args.get('q')
        try:
            clock = int(clock)
            # You'll need to implement the actual change record retrieval logic
            return jsonify({"count": 0, "changes": []})  # Placeholder
        except ValueError:
            return jsonify({"error": "Invalid clock parameter"}), 400
    except Exception as e:
        logger.error(f"Error getting changes since clock: {e}")
        return jsonify({"error": str(e)}), 500

# Entrypoint

def parse_args():
    parser = argparse.ArgumentParser(description="Web service API")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--rpc-url", default=default_rpc_url, help="Sync RPC service URL")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Set logging level")
    return parser.parse_args()


def main():
    global db_client
    args = parse_args()
    configure_logging(log_level=args.log_level)
    logger.setLevel(getattr(logging, args.log_level))
    db_client = DbClient(args.rpc_url)
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main() 