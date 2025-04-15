import unittest
import tempfile
import os
import sqlite3
from datetime import datetime

from KegDisplayDB.db.database import DatabaseManager

class TestDatabaseManager(unittest.TestCase):
    """Test class for the DatabaseManager component."""
    
    def setUp(self):
        """Set up a fresh database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test_db.db')
        self.db_manager = DatabaseManager(self.db_path)
    
    def tearDown(self):
        """Clean up resources after each test."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)
    
    def test_initialize_tables(self):
        """Test that database tables are properly initialized."""
        # Check if tables exist
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check beers table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='beers'")
            self.assertIsNotNone(cursor.fetchone(), "Beers table not created")
            
            # Check taps table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='taps'")
            self.assertIsNotNone(cursor.fetchone(), "Taps table not created")
            
            # Check beers table structure
            cursor.execute("PRAGMA table_info(beers)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            expected_columns = ['idBeer', 'Name', 'ABV', 'IBU', 'Color', 
                               'OriginalGravity', 'FinalGravity', 'Description', 
                               'Brewed', 'Kegged', 'Tapped', 'Notes']
            
            for col in expected_columns:
                self.assertIn(col, column_names, f"Column {col} missing in beers table")
    
    def test_add_beer(self):
        """Test adding a beer to the database."""
        # Basic beer with only name
        beer_id1 = self.db_manager.add_beer("Test Beer 1")
        self.assertIsNotNone(beer_id1, "Failed to add beer with only name")
        
        # Beer with all fields
        now = datetime.now()
        beer_id2 = self.db_manager.add_beer(
            name="Complete Beer",
            abv=5.5,
            ibu=45,
            color=10,
            og=1.050,
            fg=1.010,
            description="A test beer with all fields",
            brewed=now,
            kegged=now,
            tapped=now,
            notes="Test notes"
        )
        self.assertIsNotNone(beer_id2, "Failed to add beer with all fields")
        
        # Verify beers exist in database
        beer1 = self.db_manager.get_beer(beer_id1)
        self.assertEqual(beer1['Name'], "Test Beer 1", "Beer name doesn't match")
        
        beer2 = self.db_manager.get_beer(beer_id2)
        self.assertEqual(beer2['Name'], "Complete Beer", "Beer name doesn't match")
        self.assertEqual(beer2['ABV'], 5.5, "Beer ABV doesn't match")
        self.assertEqual(beer2['Description'], "A test beer with all fields", "Beer description doesn't match")
    
    def test_update_beer(self):
        """Test updating a beer in the database."""
        # Add a beer first
        beer_id = self.db_manager.add_beer("Original Beer", abv=4.0)
        
        # Update just the name
        result = self.db_manager.update_beer(beer_id, name="Updated Beer")
        self.assertTrue(result, "Update operation failed")
        
        beer = self.db_manager.get_beer(beer_id)
        self.assertEqual(beer['Name'], "Updated Beer", "Beer name not updated")
        self.assertEqual(beer['ABV'], 4.0, "Beer ABV changed unexpectedly")
        
        # Update multiple fields
        result = self.db_manager.update_beer(
            beer_id, 
            abv=6.0,
            ibu=60,
            description="New description"
        )
        self.assertTrue(result, "Update operation failed")
        
        beer = self.db_manager.get_beer(beer_id)
        self.assertEqual(beer['Name'], "Updated Beer", "Beer name changed unexpectedly")
        self.assertEqual(beer['ABV'], 6.0, "Beer ABV not updated")
        self.assertEqual(beer['IBU'], 60, "Beer IBU not updated")
        self.assertEqual(beer['Description'], "New description", "Beer description not updated")
        
        # Update non-existent beer
        result = self.db_manager.update_beer(9999, name="Non-existent")
        self.assertFalse(result, "Update of non-existent beer should fail")
    
    def test_delete_beer(self):
        """Test deleting a beer from the database."""
        # Add two beers
        beer_id1 = self.db_manager.add_beer("Delete Test 1")
        beer_id2 = self.db_manager.add_beer("Delete Test 2")
        
        # Delete the first one
        result = self.db_manager.delete_beer(beer_id1)
        self.assertTrue(result, "Delete operation failed")
        
        # Verify it's gone
        beer = self.db_manager.get_beer(beer_id1)
        self.assertIsNone(beer, "Beer was not deleted")
        
        # Verify the other beer still exists
        beer = self.db_manager.get_beer(beer_id2)
        self.assertIsNotNone(beer, "Other beer was unexpectedly deleted")
        
        # Delete non-existent beer
        result = self.db_manager.delete_beer(9999)
        self.assertFalse(result, "Delete of non-existent beer should fail")
    
    def test_get_all_beers(self):
        """Test retrieving all beers from the database."""
        # Add multiple beers
        self.db_manager.add_beer("All Test 1", abv=4.0)
        self.db_manager.add_beer("All Test 2", abv=5.0)
        self.db_manager.add_beer("All Test 3", abv=6.0)
        
        # Get all beers
        beers = self.db_manager.get_all_beers()
        self.assertEqual(len(beers), 3, "Wrong number of beers returned")
        
        # Verify beers are sorted by name (default behavior)
        self.assertEqual(beers[0]['Name'], "All Test 1", "Beers not sorted correctly")
        self.assertEqual(beers[1]['Name'], "All Test 2", "Beers not sorted correctly")
        self.assertEqual(beers[2]['Name'], "All Test 3", "Beers not sorted correctly")
    
    def test_tap_operations(self):
        """Test tap CRUD operations."""
        # Add beers
        beer_id1 = self.db_manager.add_beer("Tap Test 1")
        beer_id2 = self.db_manager.add_beer("Tap Test 2")
        
        # Add taps
        tap_id1 = self.db_manager.add_tap(1, beer_id1)
        self.assertEqual(tap_id1, 1, "Tap ID doesn't match requested ID")
        
        tap_id2 = self.db_manager.add_tap(None, beer_id2)  # Auto-assigned ID
        self.assertEqual(tap_id2, 2, "Auto-assigned tap ID incorrect")
        
        # Get tap
        tap = self.db_manager.get_tap(tap_id1)
        self.assertEqual(tap['idTap'], tap_id1, "Tap ID doesn't match")
        self.assertEqual(tap['idBeer'], beer_id1, "Beer ID doesn't match")
        
        # Update tap
        result = self.db_manager.update_tap(tap_id1, beer_id2)
        self.assertTrue(result, "Update tap operation failed")
        
        tap = self.db_manager.get_tap(tap_id1)
        self.assertEqual(tap['idBeer'], beer_id2, "Beer ID not updated")
        
        # Clear tap (set to None)
        result = self.db_manager.update_tap(tap_id1, None)
        self.assertTrue(result, "Clear tap operation failed")
        
        tap = self.db_manager.get_tap(tap_id1)
        self.assertIsNone(tap['idBeer'], "Beer ID not cleared")
        
        # Get all taps
        taps = self.db_manager.get_all_taps()
        self.assertEqual(len(taps), 2, "Wrong number of taps")
        
        # Delete tap
        result = self.db_manager.delete_tap(tap_id1)
        self.assertTrue(result, "Delete tap operation failed")
        
        tap = self.db_manager.get_tap(tap_id1)
        self.assertIsNone(tap, "Tap not deleted")
        
        taps = self.db_manager.get_all_taps()
        self.assertEqual(len(taps), 1, "Wrong number of taps after deletion")
    
    def test_get_tap_with_beer(self):
        """Test finding taps that have a specific beer."""
        # Add beers
        beer_id1 = self.db_manager.add_beer("Tap Test 1")
        beer_id2 = self.db_manager.add_beer("Tap Test 2")
        
        # Add multiple taps with the same beer
        self.db_manager.add_tap(1, beer_id1)
        self.db_manager.add_tap(2, beer_id1)
        self.db_manager.add_tap(3, beer_id2)
        
        # Find taps with beer_id1
        taps = self.db_manager.get_tap_with_beer(beer_id1)
        self.assertEqual(len(taps), 2, "Wrong number of taps with beer_id1")
        self.assertIn(1, taps, "Tap 1 not found")
        self.assertIn(2, taps, "Tap 2 not found")
        
        # Find taps with beer_id2
        taps = self.db_manager.get_tap_with_beer(beer_id2)
        self.assertEqual(len(taps), 1, "Wrong number of taps with beer_id2")
        self.assertIn(3, taps, "Tap 3 not found")
        
        # Find taps with non-existent beer
        taps = self.db_manager.get_tap_with_beer(9999)
        self.assertEqual(len(taps), 0, "Should return empty list for non-existent beer")
    
    def test_query_method(self):
        """Test the general query method."""
        # Add some test data
        self.db_manager.add_beer("Query Test 1", abv=4.0)
        self.db_manager.add_beer("Query Test 2", abv=5.0)
        
        # Test single row query
        result = self.db_manager.query(
            "SELECT * FROM beers WHERE Name = ?", 
            ("Query Test 1",),
            fetch_all=False
        )
        self.assertIsNotNone(result, "Query should return a row")
        
        # Test multi-row query
        results = self.db_manager.query(
            "SELECT * FROM beers WHERE Name LIKE ?",
            ("Query Test%",),
            fetch_all=True
        )
        self.assertEqual(len(results), 2, "Query should return 2 rows")
        
        # Test with row factory
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
            
        result = self.db_manager.query(
            "SELECT * FROM beers WHERE Name = ?",
            ("Query Test 1",),
            row_factory=dict_factory
        )
        self.assertIsInstance(result, dict, "Result should be a dictionary")
        self.assertEqual(result["Name"], "Query Test 1", "Name should match")
        
        # Test insert operation
        row_id = self.db_manager.query(
            "INSERT INTO beers (Name, ABV) VALUES (?, ?)",
            ("Query Insert Test", 7.0)
        )
        self.assertGreater(row_id, 0, "Insert should return a valid row ID")
        
        # Test update operation
        rows_affected = self.db_manager.query(
            "UPDATE beers SET ABV = ? WHERE Name = ?",
            (8.0, "Query Insert Test")
        )
        self.assertGreater(rows_affected, 0, "Update should affect some rows")
    
    def test_connection_error_handling(self):
        """Test error handling for database connections."""
        # Create a temporary directory for the test
        bad_dir = tempfile.mkdtemp()
        # Use a path inside the directory but make it read-only so we can't write to it
        os.chmod(bad_dir, 0o555)  # Read-only
        bad_path = os.path.join(bad_dir, 'bad_db.db')
        
        try:
            # This should raise an exception but not crash the program
            with self.assertRaises(sqlite3.OperationalError):
                # Just creating the manager should not fail, even with a bad path
                bad_db = DatabaseManager(bad_path)
                # But operations that require writing to the database should fail
                bad_db.add_beer("This should fail")
        finally:
            # Clean up
            os.chmod(bad_dir, 0o755)  # Make writable again for deletion
            if os.path.exists(bad_dir):
                os.rmdir(bad_dir)

if __name__ == '__main__':
    unittest.main() 