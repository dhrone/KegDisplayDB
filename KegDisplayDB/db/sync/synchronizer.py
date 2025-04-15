"""
Database synchronization module for KegDisplay.
Coordinates the overall synchronization process.
"""

import threading
import logging
import time
import os
import shutil
import socket
from datetime import datetime
import hashlib

from .protocol import SyncProtocol

logger = logging.getLogger("KegDisplay")

class DatabaseSynchronizer:
    """
    Manages database synchronization across instances.
    Coordinates network discovery, peer management, and data synchronization.
    """
    
    def __init__(self, db_manager, change_tracker, network_manager):
        """Initialize the database synchronizer
        
        Args:
            db_manager: DatabaseManager instance
            change_tracker: ChangeTracker instance
            network_manager: NetworkManager instance
        """
        self.db_manager = db_manager
        self.change_tracker = change_tracker
        self.network = network_manager
        self.protocol = SyncProtocol()
        self.peers = {}  # {ip: (version, last_seen, sync_port)}
        self.lock = threading.Lock()
        self.running = False
        self.threads = []
    
    def start(self):
        """Start the synchronization system"""
        logger.info("Starting database synchronization system")
        self.running = True
        
        # Start network listeners
        self.network.start_listeners(self.handle_message)
        
        # Start background threads
        self.threads = [
            threading.Thread(target=self._heartbeat_sender),
            threading.Thread(target=self._cleanup_peers)
        ]
        
        for thread in self.threads:
            thread.daemon = True
            thread.start()
        
        # Perform initial peer discovery
        self._initial_peer_discovery()
        
        logger.info("Database synchronization system started")
    
    def stop(self):
        """Stop the synchronization system"""
        logger.info("Stopping database synchronization system")
        self.running = False
        
        # Stop network manager
        self.network.stop()
        
        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(1.0)  # Wait up to 1 second
        
        logger.info("Database synchronization system stopped")
    
    def notify_update(self):
        """Notify other instances that a change has been made"""
        # Get current database version
        version = self.change_tracker.get_db_version()
        
        # Broadcast the update to all peers
        update_message = self.protocol.create_update_message(
            version, 
            self.network.sync_port
        )
        self.network.send_broadcast(update_message)
        
        logger.info(f"Broadcasted database update notification, version {version}")
    
    def handle_message(self, data, addr, is_sync=False):
        """Handle incoming messages
        
        Args:
            data: Message data
            addr: Address the message came from
            is_sync: Whether this is a sync connection
        """
        if is_sync:
            # Handle sync connection
            self._handle_sync_connection(data, addr)
            return
        
        # Parse the message
        message = self.protocol.parse_message(data)
        if not message:
            return
        
        message_type = message.get('type')
        if not message_type:
            return
        
        # Route message to appropriate handler
        if message_type == 'discovery':
            self._handle_discovery(message, addr)
        elif message_type == 'heartbeat':
            self._handle_heartbeat(message, addr)
        elif message_type == 'update':
            self._handle_update(message, addr)
    
    def _handle_sync_connection(self, client_socket, addr):
        """Handle incoming sync connections
        
        Args:
            client_socket: Client socket object
            addr: Address the connection came from
        """
        try:
            # Receive data from client
            data = client_socket.recv(1024)
            message = self.protocol.parse_message(data)
            
            if not message or 'type' not in message:
                client_socket.close()
                return
            
            message_type = message['type']
            logger.info(f"Received {message_type} request from {addr[0]}")
            
            if message_type == 'sync_request':
                self._handle_sync_request(client_socket, message, addr)
            elif message_type == 'full_db_request':
                self._handle_full_db_request(client_socket, message, addr)
            else:
                client_socket.close()
        except Exception as e:
            logger.error(f"Error handling sync connection: {e}")
            client_socket.close()
    
    def _handle_discovery(self, message, addr):
        """Handle discovery messages
        
        Args:
            message: Parsed message
            addr: Address the message came from
        """
        peer_ip = addr[0]
        
        # Skip messages from our own IPs
        if peer_ip in self.network.local_ips:
            return
        
        # Extract peer information
        peer_version = message.get('version')
        peer_sync_port = message.get('sync_port', self.network.sync_port)
        
        # Update peer information
        with self.lock:
            old_version = None
            if peer_ip in self.peers:
                old_version = self.peers[peer_ip][0]
            
            self.peers[peer_ip] = (peer_version, time.time(), peer_sync_port)
            
            if old_version != peer_version:
                logger.info(f"Discovered peer {peer_ip} with version {peer_version}")
    
    def _handle_heartbeat(self, message, addr):
        """Handle heartbeat messages
        
        Args:
            message: Parsed message
            addr: Address the message came from
        """
        peer_ip = addr[0]
        
        # Skip messages from our own IPs
        if peer_ip in self.network.local_ips:
            return
        
        # Extract peer information
        peer_version = message.get('version')
        peer_sync_port = message.get('sync_port', self.network.sync_port)
        
        # Update peer information
        with self.lock:
            old_version = None
            if peer_ip in self.peers:
                old_version = self.peers[peer_ip][0]
            
            self.peers[peer_ip] = (peer_version, time.time(), peer_sync_port)
            
            if old_version != peer_version:
                logger.debug(f"Updated peer {peer_ip} version to {peer_version}")
    
    def _handle_update(self, message, addr):
        """Handle update notification messages
        
        Args:
            message: Parsed message
            addr: Address the message came from
        """
        peer_ip = addr[0]
        
        # Skip messages from our own IPs
        if peer_ip in self.network.local_ips:
            return
        
        # Extract peer information
        peer_version = message.get('version')
        peer_sync_port = message.get('sync_port', self.network.sync_port)
        
        # Update peer information
        with self.lock:
            self.peers[peer_ip] = (peer_version, time.time(), peer_sync_port)
        
        # Compare versions
        our_version = self.change_tracker.get_db_version()
        if peer_version != our_version:
            logger.info(f"Detected version change from {peer_ip}, requesting sync")
            self._request_sync(peer_ip, peer_sync_port)
    
    def _handle_sync_request(self, client_socket, message, addr):
        """Handle sync request from peer
        
        Args:
            client_socket: Client socket
            message: Parsed message
            addr: Address the request came from
        """
        peer_ip = addr[0]
        
        # Get the client's last timestamp
        last_timestamp = message.get('last_timestamp', '1970-01-01T00:00:00Z')
        logger.info(f"Getting changes since {last_timestamp} for {peer_ip}")
        
        # Get changes since the client's last timestamp
        changes = self.change_tracker.get_changes_since(last_timestamp)
        
        if changes:
            logger.info(f"Found {len(changes)} changes to send to {peer_ip}")
            
            # Verify each change has content and content_hash
            for change in changes:
                if len(change) < 6:  # Should have (table_name, operation, row_id, timestamp, content, content_hash)
                    logger.warning(f"Invalid change format: {change}")
                    continue
                
                # Verify content hash matches content
                content = change[4]
                content_hash = change[5]
                if hashlib.md5(content.encode()).hexdigest() != content_hash:
                    logger.warning(f"Content hash mismatch for change: {change}")
                    continue
            
            # Send response with changes
            response = self.protocol.create_sync_response(
                self.change_tracker.get_db_version(), 
                True
            )
            client_socket.send(response)
            
            # Wait for acknowledgment
            data = client_socket.recv(1024)
            if data != self.protocol.create_ack_message():
                logger.warning(f"Invalid acknowledgment from {peer_ip}")
                client_socket.close()
                return
            
            # Send the changes
            changes_data = self.protocol.serialize_changes(changes)
            client_socket.send(changes_data)
            
            # Wait for acknowledgment
            data = client_socket.recv(1024)
            if data != self.protocol.create_ack_message():
                logger.warning(f"Invalid acknowledgment from {peer_ip}")
                client_socket.close()
                return
            
            logger.info(f"Sent changes to {peer_ip}")
        else:
            # No changes to send
            logger.info(f"No changes to send to {peer_ip}")
            response = self.protocol.create_sync_response(
                self.change_tracker.get_db_version(), 
                False
            )
            client_socket.send(response)
        
        client_socket.close()
    
    def _handle_full_db_request(self, client_socket, message, addr):
        """Handle full database request from peer
        
        Args:
            client_socket: Client socket
            message: Parsed message
            addr: Address the request came from
        """
        peer_ip = addr[0]
        
        # Check if database exists
        if os.path.exists(self.db_manager.db_path):
            db_size = os.path.getsize(self.db_manager.db_path)
            logger.info(f"Sending full database ({db_size} bytes) to {peer_ip}")
            
            # Send response with file size
            response = self.protocol.create_full_db_response(
                self.change_tracker.get_db_version(),
                db_size
            )
            client_socket.send(response)
            
            # Wait for acknowledgment
            data = client_socket.recv(1024)
            if data != self.protocol.create_ack_message():
                logger.warning(f"Invalid acknowledgment from {peer_ip}")
                client_socket.close()
                return
            
            # Send the database file
            self._send_database_file(client_socket)
            
            logger.info(f"Sent full database to {peer_ip}")
        else:
            # Database doesn't exist
            logger.info(f"Database doesn't exist, sending empty response to {peer_ip}")
            response = self.protocol.create_full_db_response(
                self.change_tracker.get_db_version(),
                0
            )
            client_socket.send(response)
        
        client_socket.close()
    
    def _send_database_file(self, client_socket):
        """Send database file over socket
        
        Args:
            client_socket: Socket to send over
        """
        try:
            with open(self.db_manager.db_path, 'rb') as f:
                bytes_sent = 0
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    client_socket.send(chunk)
                    bytes_sent += len(chunk)
            
            logger.debug(f"Sent {bytes_sent} bytes of database data")
        except Exception as e:
            logger.error(f"Error sending database file: {e}")
    
    def _request_sync(self, peer_ip, peer_sync_port):
        """Request database sync from a peer
        
        Args:
            peer_ip: IP address of the peer
            peer_sync_port: Sync port of the peer
        """
        logger.info(f"Requesting sync from peer {peer_ip}:{peer_sync_port}")
        
        try:
            # Connect to peer
            s = self.network.connect_to_peer(peer_ip, peer_sync_port)
            if not s:
                logger.error(f"Failed to connect to peer {peer_ip}:{peer_sync_port}")
                return
            
            # Get our last known timestamp
            version = self.change_tracker.get_db_version()
            timestamp = version.get('timestamp', '1970-01-01T00:00:00Z')
            
            # Send sync request
            request = self.protocol.create_sync_request(
                version,
                timestamp,
                self.network.sync_port
            )
            s.send(request)
            
            # Receive response
            response_data = s.recv(1024)
            response = self.protocol.parse_message(response_data)
            
            if not response or response.get('type') != 'sync_response':
                logger.error(f"Invalid response from peer {peer_ip}")
                s.close()
                return
            
            if response.get('has_changes', False):
                logger.info(f"Peer {peer_ip} has changes for us")
                
                # Send acknowledgment
                s.send(self.protocol.create_ack_message())
                
                # Receive changes
                changes_data = s.recv(1024 * 1024)  # Adjust buffer size as needed
                changes = self.protocol.deserialize_changes(changes_data)
                
                logger.info(f"Received {len(changes)} changes from {peer_ip}")
                
                # Send acknowledgment
                s.send(self.protocol.create_ack_message())
                
                # Apply the changes through database manager API
                logger.info(f"Applying {len(changes)} changes to our database")
                self.db_manager.apply_sync_changes(changes)
                
                # Verify versions match after sync
                peer_version = response.get('version')
                our_version = self.change_tracker.get_db_version()
                
                if peer_version.get('hash') != our_version.get('hash'):
                    logger.error(f"Version mismatch after sync with {peer_ip}")
                    logger.error(f"Peer version: {peer_version}")
                    logger.error(f"Our version: {our_version}")
                    # Rollback the changes since versions don't match
                    with self.db_manager.get_connection() as conn:
                        conn.rollback()
                else:
                    logger.info(f"Successfully synced with {peer_ip}, versions match")
                    # Only update our version if the sync was successful
                    with self.db_manager.get_connection() as conn:
                        conn.execute('''
                            INSERT OR REPLACE INTO version (id, timestamp, hash)
                            VALUES (1, ?, ?)
                        ''', (datetime.now().isoformat(), peer_version.get('hash')))
                        conn.commit()
            else:
                logger.info(f"Peer {peer_ip} has no changes for us")
            
            s.close()
            
        except Exception as e:
            logger.error(f"Sync request error: {e}")
    
    def _receive_database_file(self, socket, output_path):
        """Receive database file over socket
        
        Args:
            socket: Socket to receive from
            output_path: Path to save the database to
            
        Returns:
            bytes_received: Number of bytes received
        """
        try:
            bytes_received = 0
            with open(output_path, 'wb') as f:
                while True:
                    chunk = socket.recv(8192)
                    if not chunk:
                        break
                    bytes_received += len(chunk)
                    f.write(chunk)
            
            logger.debug(f"Received {bytes_received} bytes of database data")
            return bytes_received
        except Exception as e:
            logger.error(f"Error receiving database file: {e}")
            return 0
    
    def _request_full_database(self, peer_ip, peer_sync_port):
        """Request a full copy of the database from a peer
        
        Args:
            peer_ip: IP address of the peer
            peer_sync_port: Sync port of the peer
            
        Returns:
            success: Whether the request was successful
        """
        logger.info(f"Requesting full database from peer {peer_ip}:{peer_sync_port}")
        
        try:
            # Connect to peer
            s = self.network.connect_to_peer(peer_ip, peer_sync_port)
            if not s:
                logger.error(f"Failed to connect to peer {peer_ip}:{peer_sync_port}")
                return False
            
            # Send full database request
            request = self.protocol.create_full_db_request(
                self.change_tracker.get_db_version(),
                self.network.sync_port
            )
            s.send(request)
            
            # Receive response
            response_data = s.recv(1024)
            response = self.protocol.parse_message(response_data)
            
            if not response or response.get('type') != 'full_db_response':
                logger.error(f"Invalid response from peer {peer_ip}")
                s.close()
                return False
            
            # Get the database size
            db_size = response.get('db_size', 0)
            
            if db_size > 0:
                # Send acknowledgment
                s.send(self.protocol.create_ack_message())
                
                # Prepare temporary file to receive database
                temp_db_path = f"{self.db_manager.db_path}.temp"
                
                # Receive the database file
                bytes_received = self._receive_database_file(s, temp_db_path)
                
                if bytes_received > 0:
                    # Import the database content through the database manager API
                    success = self.db_manager.import_from_file(temp_db_path)
                    
                    # Remove the temporary file
                    if os.path.exists(temp_db_path):
                        try:
                            os.remove(temp_db_path)
                        except Exception as e:
                            logger.warning(f"Failed to remove temporary database file: {e}")
                    
                    if success:
                        logger.info(f"Successfully imported database from peer")
                        
                        # Need to re-initialize the change tracking
                        self.change_tracker.initialize_tracking()
                        
                        return True
                    else:
                        logger.error(f"Failed to import database from peer")
                        return False
                else:
                    logger.error(f"No data received from peer {peer_ip}")
                    return False
            else:
                logger.info(f"Peer {peer_ip} has an empty database")
                return False
            
        except Exception as e:
            logger.error(f"Full database request error: {e}")
            return False
        finally:
            s.close()
    
    def _initial_peer_discovery(self):
        """Discover peers and request a full database if needed"""
        logger.info("Starting initial peer discovery")
        
        # Send discovery message
        discovery_message = self.protocol.create_discovery_message(
            self.change_tracker.get_db_version(),
            self.network.sync_port
        )
        self.network.send_broadcast(discovery_message)
        
        # Wait for responses
        discovery_time = 5  # seconds
        logger.info(f"Waiting {discovery_time} seconds for peer responses...")
        time.sleep(discovery_time)
        
        # Find peer with the latest version
        latest_peer = None
        latest_port = None
        latest_version = self.change_tracker.get_db_version()
        
        with self.lock:
            logger.info(f"Found {len(self.peers)} peers during discovery")
            
            for ip, (version, _, port) in self.peers.items():
                # Check if this peer's version is different from ours
                if version.get("hash") != latest_version.get("hash"):
                    logger.info(f"Peer {ip} has different version: {version}")
                    
                    if latest_peer is None or self.change_tracker.is_newer_version(version, latest_version):
                        latest_peer = ip
                        latest_port = port
                        latest_version = version
                        logger.info(f"This is now the latest peer")
        
        if latest_peer:
            logger.info(f"Found peer with latest version: {latest_peer}, requesting full database")
            self._request_full_database(latest_peer, latest_port)
        else:
            logger.info("No peers with newer database version found")
    
    def _heartbeat_sender(self):
        """Send periodic heartbeat messages"""
        logger.info("Starting heartbeat sender")
        
        while self.running:
            try:
                # Get current version
                version = self.change_tracker.get_db_version()
                
                # Create and send heartbeat message
                heartbeat_message = self.protocol.create_heartbeat_message(
                    version,
                    self.network.sync_port
                )
                self.network.send_broadcast(heartbeat_message)
                
                # Sleep for a while
                time.sleep(5)
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    logger.error(f"Heartbeat sender error: {e}")
    
    def _cleanup_peers(self):
        """Remove peers that haven't been seen recently"""
        logger.info("Starting peer cleanup")
        
        while self.running:
            try:
                with self.lock:
                    current_time = time.time()
                    old_count = len(self.peers)
                    
                    self.peers = {
                        ip: peer_data
                        for ip, peer_data in self.peers.items()
                        if current_time - peer_data[1] < 15  # peer_data[1] is last_seen timestamp
                    }
                    
                    new_count = len(self.peers)
                    if old_count != new_count:
                        logger.debug(f"Cleaned up {old_count - new_count} inactive peers, {new_count} remaining")
                
                # Sleep for a while
                time.sleep(5)
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    logger.error(f"Peer cleanup error: {e}")
    
    def add_peer(self, peer_ip):
        """Manually add a peer by IP address
        
        Args:
            peer_ip: IP address of the peer to add
        """
        # Skip if this is our own IP
        if peer_ip in self.network.local_ips:
            logger.warning(f"Cannot add own IP {peer_ip} as peer")
            return
        
        if peer_ip and peer_ip not in self.peers:
            logger.info(f"Manually adding peer: {peer_ip}")
            
            # Default to standard primary port 5003 for manually added peers
            with self.lock:
                self.peers[peer_ip] = (
                    {"hash": "unknown", "timestamp": "1970-01-01T00:00:00Z"}, 
                    time.time(), 
                    5003
                )
            
            # Try to sync with this peer
            try:
                logger.info(f"Requesting initial sync from new peer {peer_ip}")
                self._request_full_database(peer_ip, 5003)
            except Exception as e:
                logger.error(f"Error syncing with new peer {peer_ip}: {e}")
                
    def _sync_with_peer(self, peer):
        """Synchronize changes with a peer in test mode
        
        Args:
            peer: Peer instance to sync with
        """
        try:
            # Get our last known timestamp
            our_version = self.change_tracker.get_db_version()
            last_timestamp = our_version.get('timestamp', '1970-01-01T00:00:00Z')
            
            # Get peer version
            peer_version = peer.change_tracker.get_db_version()
            
            # Skip if versions are identical
            if our_version.get('hash') == peer_version.get('hash'):
                logger.debug("Versions identical, skipping sync")
                return
            
            # Get changes from peer since our last timestamp
            changes = peer.change_tracker.get_changes_since(last_timestamp)
            
            if changes:
                logger.info(f"Got {len(changes)} changes from peer in test mode")
                
                # Create a temporary database for validation
                temp_db_path = f"{self.db_manager.db_path}.temp"
                try:
                    # Copy the current database to temp
                    shutil.copy2(self.db_manager.db_path, temp_db_path)
                    
                    # Apply the changes through database manager API
                    self.db_manager.apply_sync_changes(changes, temp_db_path)
                    
                    # Clean up temp file
                    if os.path.exists(temp_db_path):
                        os.remove(temp_db_path)
                        
                except Exception as e:
                    logger.error(f"Error applying test mode changes: {e}")
                    if os.path.exists(temp_db_path):
                        os.remove(temp_db_path)
                    raise
            else:
                logger.info("No changes to apply from peer in test mode")
                
        except Exception as e:
            logger.error(f"Test mode sync error: {e}") 