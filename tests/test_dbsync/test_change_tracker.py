import unittest
import tempfile
import os
import sqlite3
import time
from datetime import datetime, timedelta, UTC
import shutil
import json
import hashlib

from KegDisplayDB.db.database import DatabaseManager
from KegDisplayDB.db.change_tracker import ChangeTracker

class TestChangeTracker(unittest.TestCase):
    """Test class for the ChangeTracker component."""
    
    def setUp(self):
        """Set up a fresh database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test_db.db')
        self.db_manager = DatabaseManager(self.db_path)
        self.change_tracker = ChangeTracker(self.db_manager)
    
    def tearDown(self):
        """Clean up resources after each test."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)
    
    def test_initialize_tracking(self):
        """Test that change tracking tables are properly initialized."""
        # Re-initialize tracking tables
        self.change_tracker.initialize_tracking()

        # Check that tables exist
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='change_log'")
            self.assertIsNotNone(cursor.fetchone(), "change_log table missing")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='version'")
            self.assertIsNotNone(cursor.fetchone(), "version table missing")
            
            # Check change_log has the correct columns
            cursor.execute("PRAGMA table_info(change_log)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            expected_columns = ['id', 'table_name', 'operation', 'row_id', 'timestamp', 'content', 'content_hash']
            for col in expected_columns:
                self.assertIn(col, column_names, f"Column {col} missing in change_log table")
            
            # Check version table has a record
            cursor.execute("SELECT COUNT(*) FROM version")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, "version table should have exactly one record")
            
            # Check version table has a timestamp
            cursor.execute("SELECT timestamp FROM version")
            timestamp = cursor.fetchone()[0]
            self.assertIsNotNone(timestamp, "version table should have a timestamp")
    
    def test_log_change(self):
        """Test logging a change to the database."""
        # Add a beer to get a row ID
        beer_id = self.db_manager.add_beer("Change Test Beer")
        
        # Log a change for this beer
        self.change_tracker.log_change("beers", "INSERT", beer_id)
        
        # Verify the change was logged
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name, operation, row_id, content, content_hash 
                FROM change_log 
                WHERE table_name = ? AND operation = ? AND row_id = ?
            """, ("beers", "INSERT", beer_id))
            
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Change was not logged")
            self.assertEqual(result[0], "beers", "Table name doesn't match")
            self.assertEqual(result[1], "INSERT", "Operation doesn't match")
            self.assertEqual(result[2], beer_id, "Row ID doesn't match")
            self.assertIsNotNone(result[3], "Content should not be None")
            self.assertIsNotNone(result[4], "Content hash should not be None")
    
    def test_get_changes_since(self):
        """Test retrieving changes since a given timestamp."""
        # Create a timestamp in the past
        past_timestamp = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Add some beers and log changes
        beer_id1 = self.db_manager.add_beer("Change Time Test 1")
        self.change_tracker.log_change("beers", "INSERT", beer_id1)
        
        beer_id2 = self.db_manager.add_beer("Change Time Test 2")
        self.change_tracker.log_change("beers", "INSERT", beer_id2)
        
        # Update a beer
        self.db_manager.update_beer(beer_id1, abv=5.0)
        self.change_tracker.log_change("beers", "UPDATE", beer_id1)
        
        # Get changes since the past timestamp
        changes = self.change_tracker.get_changes_since(past_timestamp)
        
        # Should have 3 changes: 2 inserts and 1 update
        self.assertEqual(len(changes), 3, "Wrong number of changes returned")
        
        # First two should be inserts
        self.assertEqual(changes[0][1], "INSERT", "First change should be INSERT")
        self.assertEqual(changes[1][1], "INSERT", "Second change should be INSERT")
        
        # Last one should be an update
        self.assertEqual(changes[2][1], "UPDATE", "Third change should be UPDATE")
        
        # Get changes since now (should be empty)
        current_timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        changes = self.change_tracker.get_changes_since(current_timestamp)
        self.assertEqual(len(changes), 0, "Should return no changes since current timestamp")
    
    def test_apply_changes(self):
        """Test applying changes from a changeset."""
        # Register a beer
        beer_id = self.db_manager.add_beer("Apply Test Beer")
        
        # Create a changeset with an UPDATE operation
        timestamp = "2023-01-01T00:00:00Z"
        content_str = '{"idBeer": 1, "Name": "Updated Beer", "ABV": 7.0}'
        content_hash = hashlib.md5(content_str.encode()).hexdigest()
        
        # Test data - logical clock and node_id now included
        logical_clock = 42
        node_id = "test-node-id"
        
        # Construct a full change tuple with logical_clock and node_id
        changes = []
        
        # Create a change with 8 elements (including logical_clock and node_id)
        change = ("beers", "UPDATE", beer_id, timestamp, content_str, content_hash, logical_clock, node_id)
        changes.append(change)
        
        # Apply the changes through database manager API
        self.db_manager.apply_sync_changes(changes)
        
        # Verify the change was applied and logged
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name, operation, row_id, content, content_hash, logical_clock, node_id
                FROM change_log 
                WHERE table_name = ? AND operation = ? AND row_id = ? AND timestamp = ?
            """, ("beers", "UPDATE", beer_id, timestamp))
            
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Change was not logged")
            self.assertEqual(result[0], "beers", "Table name doesn't match")
            self.assertEqual(result[1], "UPDATE", "Operation doesn't match")
            self.assertEqual(result[2], beer_id, "Row ID doesn't match")
            self.assertEqual(result[3], content_str, "Content doesn't match")
            self.assertEqual(result[4], content_hash, "Content hash doesn't match")
            self.assertEqual(result[5], logical_clock, "Logical clock doesn't match")
            self.assertEqual(result[6], node_id, "Node ID doesn't match")
    
    def test_get_db_version(self):
        """Test retrieving the database version."""
        # Get the initial version
        version = self.change_tracker.get_db_version()
        self.assertIsNotNone(version, "Database version should not be None")
        
        # Log a change to update the version
        beer_id = self.db_manager.add_beer("Version Test Beer")
        self.change_tracker.log_change("beers", "INSERT", beer_id)
        
        # Get the updated version
        new_version = self.change_tracker.get_db_version()
        self.assertIsNotNone(new_version, "Updated database version should not be None")
        self.assertNotEqual(version, new_version, "Version should have changed after logging a change")
    
    def test_is_newer_version(self):
        """Test comparing two database versions."""
        # Create versions with logical clocks
        older = {
            "timestamp": "2023-01-01T00:00:00Z", 
            "hash": "123", 
            "logical_clock": 1, 
            "node_id": "node-a"
        }
        newer = {
            "timestamp": "2023-01-01T00:00:00Z", 
            "hash": "456", 
            "logical_clock": 2, 
            "node_id": "node-a"
        }
        
        # Check that version with higher logical clock is newer
        result = self.change_tracker.is_newer_version(newer, older)
        self.assertTrue(result, "Version with higher logical clock should be considered newer")
        
        # Check that version with lower logical clock is not newer
        result = self.change_tracker.is_newer_version(older, newer)
        self.assertFalse(result, "Version with lower logical clock should not be considered newer")
        
        # Check same logical clock but different node ID (tie-breaker)
        same_clock_node1 = {
            "timestamp": "2023-01-01T00:00:00Z", 
            "hash": "abc", 
            "logical_clock": 5, 
            "node_id": "node-a"
        }
        same_clock_node2 = {
            "timestamp": "2023-01-01T00:00:00Z", 
            "hash": "def", 
            "logical_clock": 5, 
            "node_id": "node-b"
        }
        
        # The node ID comparison should break ties (lexicographically higher wins)
        result_1 = self.change_tracker.is_newer_version(same_clock_node1, same_clock_node2)
        result_2 = self.change_tracker.is_newer_version(same_clock_node2, same_clock_node1)
        self.assertFalse(result_1 and result_2, "Only one version can be newer in a tie")
        
        # Special case with hash "0" (considered oldest)
        zero_hash = {
            "timestamp": "2023-01-01T00:00:00Z", 
            "hash": "0", 
            "logical_clock": 0, 
            "node_id": "node-empty"
        }
        result = self.change_tracker.is_newer_version(newer, zero_hash)
        self.assertTrue(result, "Any version should be newer than hash 0")
    
    def test_ensure_valid_session(self):
        """Test ensuring a valid session."""
        # This should not raise an exception
        self.change_tracker.ensure_valid_session()
        
        # Try to corrupt the session by dropping a table
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS change_log")
            cursor.execute("DROP TABLE IF EXISTS version")
            conn.commit()
        
        # In real usage, the schema needs to be recreated by the database manager
        # before change_tracker can use it
        self.db_manager.initialize_tables()
        
        # Now initialize tracking based on the recreated schema
        self.change_tracker.ensure_valid_session()
        self.change_tracker.initialize_tracking()
        
        # Verify tables exist again
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='change_log'")
            self.assertIsNotNone(cursor.fetchone(), "change_log table not recreated")
            
            # Check for necessary columns in change_log
            cursor.execute("PRAGMA table_info(change_log)")
            columns = [row[1] for row in cursor.fetchall()]
            self.assertIn("logical_clock", columns, "logical_clock column missing in change_log")
            self.assertIn("node_id", columns, "node_id column missing in change_log")
            
            # Also check for version table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='version'")
            self.assertIsNotNone(cursor.fetchone(), "version table not recreated")
            
            # Check for necessary columns in version
            cursor.execute("PRAGMA table_info(version)")
            columns = [row[1] for row in cursor.fetchall()]
            self.assertIn("logical_clock", columns, "logical_clock column missing in version")
            self.assertIn("node_id", columns, "node_id column missing in version")
    
    def test_get_table_hash(self):
        """Test generating a content hash for a table."""
        # Insert some data to hash
        beer_id = self.db_manager.add_beer("Hash Test Beer", abv=5.0, description="Test beer for hashing")
        
        # Get hash for beers table
        hash1 = self.change_tracker._get_table_hash("beers")
        self.assertIsNotNone(hash1, "Hash should not be None")
        
        # Modify the table
        self.db_manager.update_beer(beer_id, abv=6.0)
        
        # Get new hash
        hash2 = self.change_tracker._get_table_hash("beers")
        self.assertIsNotNone(hash2, "Updated hash should not be None")
        self.assertNotEqual(hash1, hash2, "Hash should change after modifying the table")
        
        # Test hash for non-existent table
        hash3 = self.change_tracker._get_table_hash("nonexistent_table")
        self.assertIsNotNone(hash3, "Hash for non-existent table should not be None")
    
    def test_prune_change_log(self):
        """Test pruning old entries from the change log."""
        # Add some beers and log changes
        for i in range(5):
            beer_id = self.db_manager.add_beer(f"Prune Test Beer {i}")
            self.change_tracker.log_change("beers", "INSERT", beer_id)
        
        # Get the count before pruning
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM change_log")
            before_count = cursor.fetchone()[0]
        
        # Set timestamps of some changes to be older (would normally happen over time)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            old_date = (datetime.now(UTC) - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
            cursor.execute("UPDATE change_log SET timestamp = ? WHERE id IN (1, 2)", (old_date,))
            conn.commit()
        
        # Prune changes older than 30 days
        days_to_keep = 30
        cutoff_date = (datetime.now(UTC) - timedelta(days=days_to_keep)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM change_log WHERE timestamp < ?", (cutoff_date,))
            count_deleted = cursor.rowcount
            conn.commit()
        
        # Get the count after pruning
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM change_log")
            after_count = cursor.fetchone()[0]
        
        # Should have 2 fewer entries
        self.assertEqual(after_count, before_count - 2, "Two entries should have been pruned")

if __name__ == '__main__':
    unittest.main() 