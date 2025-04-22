import requests
import os
import tempfile
import shutil

class DbClient:
    """
    DbClient is a thin HTTP client for interacting with the Sync RPC service.
    """
    def __init__(self, endpoint="http://127.0.0.1:5001"):
        self.base = endpoint.rstrip("/")

    # Beers
    def get_beers(self):
        r = requests.get(f"{self.base}/rpc/beers")
        r.raise_for_status()
        return r.json()

    def get_beer(self, beer_id):
        r = requests.get(f"{self.base}/rpc/beers/{beer_id}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def add_beer(self, data):
        r = requests.post(f"{self.base}/rpc/beers", json=data)
        r.raise_for_status()
        return r.json().get("beer_id")

    def update_beer(self, beer_id, data):
        r = requests.put(f"{self.base}/rpc/beers/{beer_id}", json=data)
        r.raise_for_status()
        return r.json().get("success", False)

    def delete_beer(self, beer_id):
        r = requests.delete(f"{self.base}/rpc/beers/{beer_id}")
        r.raise_for_status()
        return r.json().get("success", False)

    # Taps
    def get_taps(self):
        r = requests.get(f"{self.base}/rpc/taps")
        r.raise_for_status()
        return r.json()

    def get_tap(self, tap_id):
        r = requests.get(f"{self.base}/rpc/taps/{tap_id}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def add_tap(self, data):
        # data should include 'idBeer' key (or None)
        r = requests.post(f"{self.base}/rpc/taps", json=data)
        r.raise_for_status()
        return r.json()

    def update_tap(self, tap_id, data):
        r = requests.put(f"{self.base}/rpc/taps/{tap_id}", json=data)
        r.raise_for_status()
        return r.json().get("success", False)

    def delete_tap(self, tap_id):
        r = requests.delete(f"{self.base}/rpc/taps/{tap_id}")
        r.raise_for_status()
        return r.json().get("success", False)

    def set_tap_count(self, count):
        r = requests.post(f"{self.base}/rpc/taps/count", json={"count": count})
        r.raise_for_status()
        return r.json().get("tap_count")

    # Utility endpoints
    def clear_beers(self):
        r = requests.post(f"{self.base}/rpc/beers/clear", json={"confirmation": "CONFIRM"})
        r.raise_for_status()
        return r.json()

    def backup_beers(self):
        r = requests.get(f"{self.base}/rpc/beers/backup")
        r.raise_for_status()
        return r.text

    def import_beers(self, file_obj):
        """
        Send a CSV file to the sync service for background import.
        file_obj: werkzeug FileStorage from Flask request.files
        Returns the JSON response and status code.
        """
        # Create a temporary file to store the uploaded content
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        try:
            # Save the uploaded file to the temporary location
            file_obj.save(temp_file.name)
            temp_file.close()
            
            # Open the saved file for sending
            with open(temp_file.name, 'rb') as f:
                files = {'file': (file_obj.filename, f, file_obj.content_type)}
                r = requests.post(f"{self.base}/rpc/beers/import", files=files)
                try:
                    r.raise_for_status()
                except Exception:
                    return r.json(), r.status_code
                return r.json(), r.status_code
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_file.name)
            except:
                pass

    def get_import_status(self):
        """
        Fetch the current import status from the sync service.
        Returns the JSON response.
        """
        r = requests.get(f"{self.base}/rpc/beers/import-status")
        r.raise_for_status()
        return r.json() 