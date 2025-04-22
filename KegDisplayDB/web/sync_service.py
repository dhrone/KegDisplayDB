import os
import argparse
import logging
from flask import Flask, request, jsonify, Response
from ..db import SyncedDatabase
from ..utils.log_config import configure_logging
import io
import csv
import threading
import uuid
from datetime import datetime, UTC

# Initialize Flask app and logger
app = Flask(__name__)
logger = logging.getLogger("KegDisplayDB.sync_service")

# Default configuration
USER_HOME = os.path.expanduser("~")
DEFAULT_DB_PATH = os.path.join(USER_HOME, ".KegDisplayDB", "data", "beer.db")
DEFAULT_BROADCAST_PORT = 5002
DEFAULT_SYNC_PORT = 5003
DATA_DIR = os.path.dirname(DEFAULT_DB_PATH)

# Global import status tracking
import_status = {
    "in_progress": False,
    "last_import": {
        "timestamp": None,
        "success": None,
        "imported_count": 0,
        "errors": [],
        "status": ""
    }
}

def parse_args():
    parser = argparse.ArgumentParser(description="Sync service RPC server")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="SQLite database path")
    parser.add_argument("--broadcast-port", type=int, default=DEFAULT_BROADCAST_PORT, help="UDP broadcast port")
    parser.add_argument("--sync-port", type=int, default=DEFAULT_SYNC_PORT, help="TCP sync port")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=5001, help="Port to listen on")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Logging level")
    return parser.parse_args()

# Global state for the sync service
yargs = None
synced_db = None

# RPC endpoints for beers
@app.route("/rpc/beers", methods=["GET"])
def rpc_get_beers():
    try:
        return jsonify(synced_db.get_all_beers())
    except Exception as e:
        logger.error("Error getting beers: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers/<int:beer_id>", methods=["GET"])
def rpc_get_beer(beer_id):
    try:
        beer = synced_db.get_beer(beer_id)
        if not beer:
            return jsonify({"error": "Beer not found"}), 404
        return jsonify(beer)
    except Exception as e:
        logger.error("Error getting beer %d: %s", beer_id, e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers", methods=["POST"])
def rpc_add_beer():
    try:
        data = request.json or {}
        name = data.get("Name")
        if not name:
            return jsonify({"error": "Beer name is required"}), 400
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
        return jsonify({"success": True, "beer_id": beer_id}), 201
    except Exception as e:
        logger.error("Error adding beer: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers/<int:beer_id>", methods=["PUT"])
def rpc_update_beer(beer_id):
    try:
        data = request.json or {}
        name = data.get("Name")
        if not name:
            return jsonify({"error": "Beer name is required"}), 400
        existing = synced_db.get_beer(beer_id)
        if not existing:
            return jsonify({"error": "Beer not found"}), 404
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
        return jsonify({"error": "Failed to update beer"}), 500
    except Exception as e:
        logger.error("Error updating beer %d: %s", beer_id, e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers/<int:beer_id>", methods=["DELETE"])
def rpc_delete_beer(beer_id):
    try:
        existing = synced_db.get_beer(beer_id)
        if not existing:
            return jsonify({"error": "Beer not found"}), 404
        success = synced_db.delete_beer(beer_id)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to delete beer"}), 500
    except Exception as e:
        logger.error("Error deleting beer %d: %s", beer_id, e)
        return jsonify({"error": str(e)}), 500

# RPC endpoints for taps
@app.route("/rpc/taps", methods=["GET"])
def rpc_get_taps():
    try:
        return jsonify(synced_db.get_all_taps())
    except Exception as e:
        logger.error("Error getting taps: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/taps/<int:tap_id>", methods=["GET"])
def rpc_get_tap(tap_id):
    try:
        tap = synced_db.get_tap(tap_id)
        if not tap:
            return jsonify({"error": "Tap not found"}), 404
        return jsonify(tap)
    except Exception as e:
        logger.error("Error getting tap %d: %s", tap_id, e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/taps", methods=["POST"])
def rpc_add_tap():
    try:
        data = request.json or {}
        beer_id = data.get('idBeer')
        taps = synced_db.get_all_taps()
        next_id = 1 if not taps else max(t['idTap'] for t in taps) + 1
        tap_id = synced_db.add_tap(next_id, beer_id)
        return jsonify(synced_db.get_tap(tap_id)), 201
    except Exception as e:
        logger.error("Error adding tap: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/taps/<int:tap_id>", methods=["PUT"])
def rpc_update_tap(tap_id):
    try:
        data = request.json or {}
        beer_id = data.get('beer_id')
        success = synced_db.update_tap(tap_id, beer_id)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to update tap"}), 500
    except Exception as e:
        logger.error("Error updating tap %d: %s", tap_id, e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/taps/<int:tap_id>", methods=["DELETE"])
def rpc_delete_tap(tap_id):
    try:
        success = synced_db.delete_tap(tap_id)
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "Failed to delete tap"}), 500
    except Exception as e:
        logger.error("Error deleting tap %d: %s", tap_id, e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/taps/count", methods=["POST"])
def rpc_set_tap_count():
    try:
        data = request.json or {}
        count = data.get('count')
        if not isinstance(count, int) or count < 1:
            return jsonify({"error": "Valid tap count is required"}), 400
        success = synced_db.set_tap_count(count)
        if success:
            return jsonify({"success": True, "tap_count": count})
        return jsonify({"error": "Failed to set tap count"}), 500
    except Exception as e:
        logger.error("Error setting tap count: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers/backup", methods=["GET"])
def rpc_backup_beers():
    try:
        beers = synced_db.get_all_beers()
        columns = beers[0].keys() if beers else []
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for beer in beers:
            writer.writerow([beer.get(col) for col in columns])
        csv_data = output.getvalue()
        output.close()
        return Response(csv_data, mimetype='text/csv', headers={'Content-Disposition':'attachment; filename=beers_backup.csv'})
    except Exception as e:
        logger.error("Error backup beers: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers/clear", methods=["POST"])
def rpc_clear_beers():
    try:
        data = request.json or {}
        if data.get('confirmation') != 'CONFIRM':
            return jsonify({"error": "Confirmation required"}), 400
        count = synced_db.clear_all_beers()
        return jsonify({"success": True, "message": f"Cleared {count} beers"})
    except Exception as e:
        logger.error("Error clearing beers: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/rpc/beers/import", methods=["POST"])
def rpc_import_beers():
    global import_status
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    if not file.filename.lower().endswith('.csv'):
        return jsonify({"error": "File must be a CSV"}), 400
    import_status["in_progress"] = True
    import_status["last_import"] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "success": None,
        "imported_count": 0,
        "errors": [],
        "status": "Reading file..."
    }
    temp_path = os.path.join(DATA_DIR, f"temp_import_{uuid.uuid4()}.csv")

    def background_import():
        global import_status
        try:
            # Save file to disk
            with open(temp_path, 'wb') as f:
                while True:
                    chunk = file.stream.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
            import_status["last_import"]["status"] = "Parsing CSV..."
            # Parse CSV
            with open(temp_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                if 'Name' not in reader.fieldnames:
                    raise ValueError("Required field 'Name' missing")
                data_list = []
                for row in reader:
                    if not row.get('Name'):
                        continue
                    for fld in ['ABV','IBU','Color','OriginalGravity','FinalGravity']:
                        if fld in row and row[fld].strip() == '':
                            row[fld] = None
                        elif fld in row:
                            try:
                                row[fld] = float(row[fld])
                            except:
                                row[fld] = None
                    data_list.append(row)
            import_status["last_import"]["status"] = "Importing to DB..."
            # Perform import via SyncedDatabase
            imported_count, errors = synced_db.import_beers_from_data(data_list)
            import_status["last_import"].update({
                "imported_count": imported_count,
                "errors": errors[:10],
                "success": True,
                "status": f"Imported {imported_count} beers"
            })
        except Exception as e:
            import_status["last_import"].update({
                "success": False,
                "errors": [str(e)],
                "status": "Failed"
            })
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            import_status["in_progress"] = False

    threading.Thread(target=background_import, daemon=True).start()
    return jsonify({"success": True, "message": "Import started"}), 202

@app.route("/rpc/beers/import-status", methods=["GET"])
def rpc_import_status():
    resp = {
        "in_progress": import_status["in_progress"],
        "last_import": import_status["last_import"],
        "current_time": datetime.now(UTC).isoformat()
    }
    return jsonify(resp)

# Main entrypoint
def main():
    global args, synced_db
    args = parse_args()
    configure_logging(log_level=args.log_level)
    logger.setLevel(getattr(logging, args.log_level))
    args.db_path = os.path.expanduser(args.db_path)
    synced_db = SyncedDatabase(db_path=args.db_path, broadcast_port=args.broadcast_port, sync_port=args.sync_port, test_mode=False)
    logger.info(f"Sync service running on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, threaded=False)

if __name__ == "__main__":
    main() 