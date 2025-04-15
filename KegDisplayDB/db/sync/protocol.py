"""
Protocol definition module for KegDisplay database synchronization.
Defines message formats and parsing logic for sync protocol.
"""

import json
import logging

logger = logging.getLogger("KegDisplay")

class SyncProtocol:
    """
    Defines the protocol for database synchronization.
    Handles message creation, parsing, and protocol-specific logic.
    """
    
    @staticmethod
    def create_discovery_message(version, sync_port):
        """Create a discovery message
        
        Args:
            version: Current database version
            sync_port: Port used for sync connections
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'discovery',
            'version': version,
            'sync_port': sync_port
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_heartbeat_message(version, sync_port):
        """Create a heartbeat message
        
        Args:
            version: Current database version
            sync_port: Port used for sync connections
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'heartbeat',
            'version': version,
            'sync_port': sync_port
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_update_message(version, sync_port):
        """Create an update notification message
        
        Args:
            version: Current database version
            sync_port: Port used for sync connections
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'update',
            'version': version,
            'sync_port': sync_port
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_sync_request(version, last_timestamp, sync_port):
        """Create a sync request message
        
        Args:
            version: Current database version
            last_timestamp: Last known timestamp
            sync_port: Port used for sync connections
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'sync_request',
            'version': version,
            'last_timestamp': last_timestamp,
            'sync_port': sync_port
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_sync_response(version, has_changes):
        """Create a sync response message
        
        Args:
            version: Current database version
            has_changes: Whether there are changes to sync
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'sync_response',
            'version': version,
            'has_changes': has_changes
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_full_db_request(version, sync_port):
        """Create a full database request message
        
        Args:
            version: Current database version
            sync_port: Port used for sync connections
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'full_db_request',
            'version': version,
            'sync_port': sync_port
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_full_db_response(version, db_size):
        """Create a full database response message
        
        Args:
            version: Current database version
            db_size: Size of the database in bytes
            
        Returns:
            bytes: Encoded message
        """
        message = {
            'type': 'full_db_response',
            'version': version,
            'db_size': db_size
        }
        return json.dumps(message).encode()
    
    @staticmethod
    def create_ack_message():
        """Create a simple acknowledgment message
        
        Returns:
            bytes: Encoded message
        """
        return b'ACK'
    
    @staticmethod
    def parse_message(data):
        """Parse a received message
        
        Args:
            data: Received data (bytes)
            
        Returns:
            dict: Parsed message or None if invalid
        """
        try:
            # Check if it's a simple ACK
            if data == b'ACK':
                return {'type': 'ack'}
            
            # Try to parse as JSON
            message = json.loads(data.decode())
            return message
        except json.JSONDecodeError as e:
            logger.error(f"Invalid message format: {e}")
            return None
        except UnicodeDecodeError as e:
            logger.error(f"Error decoding message: {e}")
            return None
        
    @staticmethod
    def serialize_changes(changes):
        """Serialize changes for transmission
        
        Args:
            changes: List of changes
            
        Returns:
            bytes: Serialized changes
        """
        # Convert SQLite Row objects to serializable lists
        serializable_changes = []
        for change in changes:
            # If the change is a sqlite3.Row object, convert it to a tuple
            if hasattr(change, 'keys'):  # This is how we detect a Row object
                serializable_changes.append(tuple(change))
            else:
                serializable_changes.append(change)
        
        return json.dumps(serializable_changes).encode()
    
    @staticmethod
    def deserialize_changes(data):
        """Deserialize received changes
        
        Args:
            data: Received data (bytes)
            
        Returns:
            list: Deserialized changes
        """
        try:
            return json.loads(data.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Error deserializing changes: {e}")
            return [] 