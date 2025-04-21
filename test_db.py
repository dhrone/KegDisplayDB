import tempfile
import os
import sys
import shutil

# Add the project root to the path
sys.path.insert(0, os.getcwd())

from KegDisplayDB.db.database import DatabaseManager

# Set up a temporary database
temp_dir = tempfile.mkdtemp()
db_path = os.path.join(temp_dir, 'test_db.db')
print(f"Creating database at {db_path}")

# Initialize the database manager
db_manager = DatabaseManager(db_path)

try:
    # Test adding a beer
    print("Testing add_beer...")
    beer_id = db_manager.add_beer("Test Beer")
    print(f"Added beer with ID: {beer_id}")
    
    # Test retrieving the beer
    beer = db_manager.get_beer(beer_id)
    print(f"Retrieved beer: {beer}")
    
    # Check if we got the correct beer
    assert beer['Name'] == "Test Beer", f"Expected 'Test Beer', got '{beer['Name']}'"
    
    print("All tests passed!")
except Exception as e:
    print(f"ERROR: {e}")
finally:
    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir) 