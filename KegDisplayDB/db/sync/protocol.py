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
            
            # Show first part of the data for debugging
            preview = data[:50].decode('utf-8', errors='replace')
            logger.debug(f"Parsing message data: {preview}...")
            
            # Try to parse as JSON
            message = json.loads(data.decode())
            
            # Log the message type for debugging
            message_type = message.get('type', 'unknown')
            logger.debug(f"Successfully parsed message of type: {message_type}")
            
            # Special logging for update messages
            if message_type == 'update':
                logger.info(f"Parsed UPDATE message: {message}")
                
            return message
        except json.JSONDecodeError as e:
            # More detailed logging for JSON errors
            logger.error(f"Invalid message format: {e}")
            logger.error(f"Message data (first 100 bytes): {repr(data[:100])}")
            return None
        except UnicodeDecodeError as e:
            logger.error(f"Error decoding message: {e}")
            logger.error(f"Message data (first 100 bytes hex): {data[:100].hex()}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing message: {e}")
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