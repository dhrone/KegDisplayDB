"""
Network communication module for KegDisplay database synchronization.
Handles socket management and network communications.
"""

import socket
import logging
import threading
import time
import json

logger = logging.getLogger("KegDisplay")

class NetworkManager:
    """
    Manages network communication for database synchronization.
    Handles socket setup, network discovery, and connection management.
    """
    
    def __init__(self, broadcast_port=5002, sync_port=5003):
        """Initialize the network manager
        
        Args:
            broadcast_port: Port for UDP broadcast messages
            sync_port: Port for TCP sync connections
        """
        self.broadcast_port = broadcast_port
        self.sync_port = sync_port
        self.broadcast_socket = None
        self.sync_socket = None
        self.local_ips = self._get_all_local_ips()
        self.message_handler = None  # Will be set by start_listeners
        self.running = False
        self.threads = []
        
        logger.debug(f"Identified local IP addresses: {', '.join(self.local_ips)}")
    
    def setup_sockets(self):
        """Initialize network sockets"""
        logger.info("Setting up network sockets")
        
        # Broadcast socket for discovery
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            logger.info(f"Binding broadcast socket to port {self.broadcast_port}")
            self.broadcast_socket.bind(('', self.broadcast_port))
            logger.info(f"Successfully bound broadcast socket to port {self.broadcast_port}")
        except Exception as e:
            logger.error(f"Error binding broadcast socket: {e}")
            raise
        
        # TCP socket for database sync
        self.sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            logger.info(f"Binding sync socket to port {self.sync_port}")
            self.sync_socket.bind(('', self.sync_port))
            self.sync_socket.listen(5)
            logger.info(f"Successfully bound sync socket to port {self.sync_port}")
        except Exception as e:
            logger.error(f"Error binding sync socket: {e}")
            raise
    
    def start_listeners(self, message_handler):
        """Start network listener threads
        
        Args:
            message_handler: Callback function to handle received messages
        """
        if self.broadcast_socket is None or self.sync_socket is None:
            self.setup_sockets()
        
        self.message_handler = message_handler
        self.running = True
        
        self.threads = [
            threading.Thread(target=self._broadcast_listener),
            threading.Thread(target=self._sync_listener)
        ]
        
        for thread in self.threads:
            thread.daemon = True
            thread.start()
        
        logger.info("Network listeners started")
    
    def stop(self):
        """Stop the network manager and close sockets"""
        logger.info("Stopping network manager")
        self.running = False
        
        # Close broadcast socket safely
        try:
            if self.broadcast_socket:
                logger.info("Closing broadcast socket")
                self.broadcast_socket.shutdown(socket.SHUT_RDWR)
                self.broadcast_socket.close()
                logger.info("Broadcast socket closed")
        except Exception as e:
            logger.warning(f"Error closing broadcast socket: {e}")
        
        # Close sync socket safely
        try:
            if self.sync_socket:
                logger.info("Closing sync socket")
                self.sync_socket.shutdown(socket.SHUT_RDWR)
                self.sync_socket.close()
                logger.info("Sync socket closed")
        except Exception as e:
            logger.warning(f"Error closing sync socket: {e}")
        
        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(1.0)  # Wait up to 1 second
        
        logger.info("Network manager stopped")
    
    def send_broadcast(self, message):
        """Send a broadcast message
        
        Args:
            message: Message to broadcast (bytes)
        """
        if self.broadcast_socket is None:
            logger.info("Setting up sockets before broadcast")
            self.setup_sockets()
        
        try:
            # Try to decode the first part of the message for logging
            try:
                msg_preview = message[:50].decode('utf-8', errors='replace')
                logger.info(f"Broadcasting message ({len(message)} bytes): {msg_preview}...")
            except Exception as e:
                logger.info(f"Broadcasting message ({len(message)} bytes), preview decode failed: {e}")
            
            self.broadcast_socket.sendto(message, ('<broadcast>', self.broadcast_port))
            logger.debug(f"Broadcast message sent to port {self.broadcast_port}")
        except Exception as e:
            logger.error(f"Error sending broadcast: {e}")
    
    def send_direct(self, ip, port, message):
        """Send a direct message to a specific IP:port
        
        Args:
            ip: IP address to send to
            port: Port to send to
            message: Message to send (bytes)
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(message, (ip, port))
                logger.debug(f"Direct message sent to {ip}:{port}")
        except Exception as e:
            logger.error(f"Error sending direct message to {ip}:{port}: {e}")
    
    def connect_to_peer(self, peer_ip, peer_port, timeout=5):
        """Create a connection to a peer for TCP communication
        
        Args:
            peer_ip: IP address of the peer
            peer_port: Port of the peer
            timeout: Connection timeout in seconds
            
        Returns:
            socket: Connected socket or None if failed
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((peer_ip, peer_port))
            logger.debug(f"Connected to peer {peer_ip}:{peer_port}")
            return s
        except Exception as e:
            logger.error(f"Error connecting to peer {peer_ip}:{peer_port}: {e}")
            return None
    
    def _broadcast_listener(self):
        """Listen for broadcast messages from peers"""
        logger.info(f"Starting broadcast listener on port {self.broadcast_port}")
        recv_count = 0
        last_log_time = time.time()
        
        while self.running:
            try:
                data, addr = self.broadcast_socket.recvfrom(1024)
                recv_count += 1
                
                # Periodic logging to ensure the listener is active
                current_time = time.time()
                if current_time - last_log_time > 60:  # Log activity every minute
                    logger.info(f"Broadcast listener active, received {recv_count} messages in the last minute")
                    recv_count = 0
                    last_log_time = current_time
                
                # Skip messages from our own IP
                if addr[0] not in self.local_ips:
                    logger.info(f"Received broadcast from {addr[0]}, data length: {len(data)}")
                    
                    # Try to log the first part of the message for debugging
                    try:
                        msg_preview = data[:50].decode('utf-8', errors='replace')
                        logger.info(f"Message preview: {msg_preview}...")
                        
                        # Try to parse the message to check if it's an update
                        try:
                            msg_data = json.loads(data.decode('utf-8'))
                            if msg_data.get('type') == 'update':
                                logger.info(f"Received UPDATE message from {addr[0]}: {msg_data}")
                        except json.JSONDecodeError:
                            logger.debug(f"Could not parse message as JSON")
                            
                    except Exception as e:
                        logger.debug(f"Could not decode message preview: {e}")
                    
                    # Call the message handler if set
                    if self.message_handler:
                        logger.debug(f"Calling message handler for broadcast from {addr[0]}")
                        self.message_handler(data, addr)
                    else:
                        logger.warning(f"No message handler set for broadcast from {addr[0]}")
                else:
                    logger.debug(f"Ignoring broadcast from own IP {addr[0]}")
                    
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    logger.error(f"Broadcast listener error: {e}")
    
    def _sync_listener(self):
        """Listen for sync connections"""
        logger.info(f"Starting sync listener on port {self.sync_port}")
        
        # Make sync_socket non-blocking for clean shutdown
        self.sync_socket.settimeout(1.0)
        
        while self.running:
            try:
                client, addr = self.sync_socket.accept()
                logger.info(f"Accepted sync connection from {addr[0]}")
                
                # Handle the connection in a new thread
                threading.Thread(
                    target=self._handle_sync_connection,
                    args=(client, addr)
                ).start()
            except socket.timeout:
                continue  # This is expected due to the timeout we set
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    logger.error(f"Sync listener error: {e}")
    
    def _handle_sync_connection(self, client, addr):
        """Handle an incoming sync connection
        
        Args:
            client: Client socket
            addr: Address of the client
        """
        try:
            # Call the message handler if set
            if self.message_handler:
                self.message_handler(client, addr, is_sync=True)
            else:
                # If no handler, close the connection
                client.close()
        except Exception as e:
            logger.error(f"Error handling sync connection from {addr[0]}: {e}")
            client.close()
    
    def _get_local_ip(self):
        """Get the primary local IP address
        
        Returns:
            ip: The primary local IP address
        """
        try:
            # Create a socket that doesn't actually connect
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            try:
                # This doesn't actually establish a connection
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
                s.close()
                
                if ip != '127.0.0.1' and ip != '127.0.1.1':
                    logger.info(f"Found local IP: {ip}")
                    return ip
            except Exception as e:
                logger.error(f"Error getting IP via socket: {e}")
            
            # Try hostname
            try:
                hostname = socket.gethostname()
                ip = socket.gethostbyname(hostname)
                
                if ip != '127.0.0.1' and ip != '127.0.1.1':
                    logger.info(f"Found local IP via hostname: {ip}")
                    return ip
            except Exception as e:
                logger.error(f"Error getting IP via hostname: {e}")
            
            # Fallback
            logger.warning("Could not find a suitable local IP, falling back to 127.0.0.1")
            return '127.0.0.1'
        except Exception as e:
            logger.error(f"Error getting local IP: {e}")
            return '127.0.0.1'
    
    def _get_all_local_ips(self):
        """Get a list of all local IP addresses
        
        Returns:
            ips: List of local IP addresses
        """
        local_ips = ['127.0.0.1', '127.0.1.1']  # Always include localhost
        
        try:
            # Get the "main" IP first
            main_ip = self._get_local_ip()
            if main_ip not in local_ips:
                local_ips.append(main_ip)
            
            # Try socket connections to different addresses to find interfaces
            for test_addr in ['8.8.8.8', '1.1.1.1', '192.168.1.1']:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    s.connect((test_addr, 80))
                    ip = s.getsockname()[0]
                    if ip not in local_ips:
                        local_ips.append(ip)
                except:
                    pass
                finally:
                    s.close()
            
            # Try hostname
            try:
                hostname = socket.gethostname()
                try:
                    ip = socket.gethostbyname(hostname)
                    if ip not in local_ips:
                        local_ips.append(ip)
                except:
                    pass
                
                # Try all addresses returned by gethostbyname_ex
                try:
                    _, _, ips = socket.gethostbyname_ex(hostname)
                    for ip in ips:
                        if ip not in local_ips:
                            local_ips.append(ip)
                except:
                    pass
            except:
                pass
            
        except Exception as e:
            logger.error(f"Error getting all local IPs: {e}")
        
        return local_ips 