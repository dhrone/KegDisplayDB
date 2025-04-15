# KegDisplayDB

KegDisplayDB is a Python package that provides the database, synchronization, and web components for the KegDisplay project. It handles beer and tap data storage, multi-instance synchronization, and web-based management.

## Features

- **SQLite Database:** Stores information about beers and taps
- **Database Synchronization:** Automatically syncs data between multiple instances
- **Web Interface:** Provides a web-based UI for managing beers and taps
- **REST API:** Offers programmatic access to beer and tap data

## Installation

This package uses Poetry for dependency management. To install:

```bash
# Clone the repository
git clone <repository-url>
cd KegDisplayDB

# Install with Poetry
poetry install
```

## Usage

### Using the Database

```python
from KegDisplayDB.db import SyncedDatabase

# Create a database instance
db = SyncedDatabase('path/to/database.db')

# Add a new beer
beer_id = db.add_beer(
    name="IPA Example", 
    abv=6.5, 
    ibu=65, 
    description="A hoppy IPA with citrus notes"
)

# Assign a beer to a tap
tap_id = db.add_tap(1, beer_id)

# Get all beers
beers = db.get_all_beers()

# Get all taps with beer info
taps = db.get_all_taps()
```

### Web Interface

To start the web server:

```bash
# Using built-in Flask server (development only)
poetry run python -m KegDisplayDB.web.app

# Using gunicorn (production)
poetry run gunicorn KegDisplayDB.web.app:app
```

The web interface will be available at http://localhost:5000 by default.

## Testing

Run the test suite with pytest:

```bash
poetry run pytest
```

## Architecture

KegDisplayDB consists of several components:

- `db`: Core database functionality
  - `database.py`: Basic database operations
  - `change_tracker.py`: Tracks changes for synchronization
  - `sync/`: Database synchronization components
- `web`: Web interface and API
  - `app.py`: Flask application
  - `models/`: Data models
  - `routes/`: Web routes and API endpoints
  - `templates/`: HTML templates
- `utils`: Utility functions

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
