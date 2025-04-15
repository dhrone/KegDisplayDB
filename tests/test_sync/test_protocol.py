import unittest
import json
from unittest import mock

from KegDisplayDB.db.sync.protocol import SyncProtocol

class TestSyncProtocol(unittest.TestCase):
    """Test suite for the SyncProtocol class."""
    
    def setUp(self):
        """Set up the test case."""
        self.protocol = SyncProtocol()
        
        # Test data
        self.test_version = {"hash": "abc123", "timestamp": "2023-01-01T00:00:00Z"}
        self.test_sync_port = 5005
        self.test_last_timestamp = "2023-01-01T00:00:00Z"
        self.test_has_changes = True
        self.test_db_size = 1024
        self.test_changes = [
            ('beers', 'INSERT', 1, '2023-01-01T00:00:00Z', 'abc123'),
            ('taps', 'UPDATE', 2, '2023-01-01T00:00:00Z', 'def456')
        ]
    
    def test_create_discovery_message(self):
        """Test creation of a discovery message."""
        message = self.protocol.create_discovery_message(self.test_version, self.test_sync_port)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'discovery')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['sync_port'], self.test_sync_port)
    
    def test_create_heartbeat_message(self):
        """Test creation of a heartbeat message."""
        message = self.protocol.create_heartbeat_message(self.test_version, self.test_sync_port)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'heartbeat')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['sync_port'], self.test_sync_port)
    
    def test_create_update_message(self):
        """Test creation of an update message."""
        message = self.protocol.create_update_message(self.test_version, self.test_sync_port)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'update')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['sync_port'], self.test_sync_port)
    
    def test_create_sync_request(self):
        """Test creation of a sync request message."""
        message = self.protocol.create_sync_request(self.test_version, self.test_last_timestamp, self.test_sync_port)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'sync_request')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['last_timestamp'], self.test_last_timestamp)
        self.assertEqual(parsed['sync_port'], self.test_sync_port)
    
    def test_create_sync_response(self):
        """Test creation of a sync response message."""
        message = self.protocol.create_sync_response(self.test_version, self.test_has_changes)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'sync_response')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['has_changes'], self.test_has_changes)
    
    def test_create_full_db_request(self):
        """Test creation of a full database request message."""
        message = self.protocol.create_full_db_request(self.test_version, self.test_sync_port)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'full_db_request')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['sync_port'], self.test_sync_port)
    
    def test_create_full_db_response(self):
        """Test creation of a full database response message."""
        message = self.protocol.create_full_db_response(self.test_version, self.test_db_size)
        parsed = json.loads(message.decode('utf-8'))
        
        self.assertEqual(parsed['type'], 'full_db_response')
        self.assertEqual(parsed['version'], self.test_version)
        self.assertEqual(parsed['db_size'], self.test_db_size)
    
    def test_create_ack_message(self):
        """Test creation of an acknowledgment message."""
        message = self.protocol.create_ack_message()
        
        self.assertEqual(message, b"ACK")
    
    def test_parse_message_ack(self):
        """Test parsing an acknowledgment message."""
        message = self.protocol.parse_message(b"ACK")
        
        self.assertEqual(message, {'type': 'ack'})
    
    def test_parse_message_json(self):
        """Test parsing a valid JSON message."""
        test_message = {"type": "test", "data": "test_data"}
        message_bytes = json.dumps(test_message).encode('utf-8')
        
        parsed = self.protocol.parse_message(message_bytes)
        
        self.assertEqual(parsed, test_message)
    
    def test_parse_message_invalid_json(self):
        """Test parsing an invalid JSON message."""
        message_bytes = b"{invalid_json"
        
        with mock.patch('KegDisplayDB.db.sync.protocol.logger') as mock_logger:
            parsed = self.protocol.parse_message(message_bytes)
            
            self.assertIsNone(parsed)
            mock_logger.error.assert_called()
    
    def test_parse_message_non_utf8(self):
        """Test parsing a non-UTF8 message."""
        message_bytes = b"\xff\xfe\xfd"  # Invalid UTF-8
        
        with mock.patch('KegDisplayDB.db.sync.protocol.logger') as mock_logger:
            parsed = self.protocol.parse_message(message_bytes)
            
            self.assertIsNone(parsed)
            mock_logger.error.assert_called()
    
    def test_serialize_changes(self):
        """Test serializing changes."""
        serialized = self.protocol.serialize_changes(self.test_changes)
        self.assertIsInstance(serialized, bytes)
        
        # Deserialize and verify
        decoded = serialized.decode('utf-8')
        deserialized = json.loads(decoded)
        
        # Convert tuples to lists for comparison since JSON doesn't preserve tuple types
        expected_changes = [list(change) for change in self.test_changes]
        self.assertEqual(deserialized, expected_changes)
    
    def test_deserialize_changes(self):
        """Test deserializing changes."""
        # First serialize the changes
        changes_list = [list(change) for change in self.test_changes]
        serialized = json.dumps(changes_list).encode('utf-8')
        
        # Then deserialize
        with mock.patch('KegDisplayDB.db.sync.protocol.logger') as mock_logger:
            deserialized = self.protocol.deserialize_changes(serialized)
            
            # Convert back to lists for comparison since the implementation may return lists
            deserialized_lists = [list(change) for change in deserialized]
            expected_changes = [list(change) for change in self.test_changes]
            self.assertEqual(deserialized_lists, expected_changes)
    
    def test_deserialize_changes_invalid_json(self):
        """Test deserializing invalid JSON changes."""
        invalid_json = b"{invalid_json"
        
        with mock.patch('KegDisplayDB.db.sync.protocol.logger') as mock_logger:
            deserialized = self.protocol.deserialize_changes(invalid_json)
            
            self.assertEqual(deserialized, [])
            mock_logger.error.assert_called()
    
    def test_deserialize_changes_non_utf8(self):
        """Test deserializing non-UTF8 changes."""
        non_utf8 = b"\xff\xfe\xfd"  # Invalid UTF-8
        
        with mock.patch('KegDisplayDB.db.sync.protocol.logger') as mock_logger:
            deserialized = self.protocol.deserialize_changes(non_utf8)
            
            self.assertEqual(deserialized, [])
            mock_logger.error.assert_called()

if __name__ == '__main__':
    unittest.main() 