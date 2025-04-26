import unittest
import os
import tempfile
import shutil
import json
from unittest import mock

from KegDisplayDB.db.sync.synchronizer import DatabaseSynchronizer
from KegDisplayDB.db.sync.protocol import SyncProtocol
from KegDisplayDB.db.database import DatabaseManager
from KegDisplayDB.db.change_tracker import ChangeTracker

class TestFullDatabaseRequest(unittest.TestCase):
    """Test class specifically for the full database request handling."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock dependencies
        self.mock_db_manager = mock.MagicMock()
        self.mock_change_tracker = mock.MagicMock()
        self.mock_network_manager = mock.MagicMock()
        
        # Set test version information
        self.test_version = {
            "hash": "abc123", 
            "timestamp": "2023-01-01T00:00:00Z", 
            "logical_clock": 10, 
            "node_id": "test-node-id"
        }
        
        # Mock important methods
        self.mock_change_tracker.get_db_version.return_value = self.test_version
        
        # Create synchronizer with mock dependencies
        self.synchronizer = DatabaseSynchronizer(
            self.mock_db_manager,
            self.mock_change_tracker,
            self.mock_network_manager,
            socket_timeout=1
        )
        
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "test.db")
        
        # Mock db_path
        self.mock_db_manager.db_path = self.test_db_path
        
        # Create an empty file for testing
        with open(self.test_db_path, 'w') as f:
            f.write('test database content')
    
    def tearDown(self):
        """Clean up after the test."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_handle_full_db_request_db_exists(self):
        """Test handling a full database request from a peer when DB exists."""
        # Create mock client socket
        mock_client = mock.MagicMock()
        test_addr = ('192.168.1.10', 5000)
        
        # Create a test message
        test_message = {
            'type': 'full_db_request',
            'version': self.test_version,
            'sync_port': 5005
        }
        
        # Mock needed methods
        self.synchronizer._await_ack = mock.MagicMock(return_value=True)
        self.synchronizer._update_version_cache = mock.MagicMock(return_value=self.test_version)
        self.synchronizer._send_database_file = mock.MagicMock()
        
        # Call the method
        self.synchronizer._handle_full_db_request(mock_client, test_message, test_addr)
        
        # Check if the response was sent (full_db_response)
        mock_client.sendall.assert_called()
        
        # Check if _send_database_file was called
        self.synchronizer._send_database_file.assert_called_with(mock_client)
        
        # Check if the client socket was closed
        mock_client.close.assert_called()
    
    def test_handle_full_db_request_db_missing(self):
        """Test handling a full database request when DB doesn't exist."""
        # Remove the test DB file
        os.remove(self.test_db_path)
        
        # Create mock client socket
        mock_client = mock.MagicMock()
        test_addr = ('192.168.1.10', 5000)
        
        # Create a test message
        test_message = {
            'type': 'full_db_request',
            'version': self.test_version,
            'sync_port': 5005
        }
        
        # Mock methods
        self.synchronizer._await_ack = mock.MagicMock()
        self.synchronizer._update_version_cache = mock.MagicMock(return_value=self.test_version)
        self.synchronizer._send_database_file = mock.MagicMock()
        
        # Call the method
        self.synchronizer._handle_full_db_request(mock_client, test_message, test_addr)
        
        # Check if the response was sent with size 0
        self.assertTrue(mock_client.sendall.called)
        
        # Verify client was closed
        mock_client.close.assert_called_once()
        
        # Verify _send_database_file was not called (since DB doesn't exist)
        # Use call_count to check if the method was called
        self.assertEqual(self.synchronizer._send_database_file.call_count, 0)

if __name__ == '__main__':
    unittest.main() 