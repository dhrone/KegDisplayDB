#!/usr/bin/env python3
"""
Patch for NetworkManager to improve error logging.
This can help debug silent socket binding failures.
"""

def improved_setup_sockets(self):
    """Enhanced version of setup_sockets with better error logging"""
    import socket
    import logging
    
    logger = logging.getLogger("KegDisplay")
    logger.info("Setting up network sockets with enhanced logging")
    
    # Test UDP socket binding
    try:
        logger.info("Creating UDP broadcast socket...")
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logger.info("✓ UDP socket created")
        
        logger.info("Setting UDP socket options...")
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        logger.info("✓ UDP socket options set")
        
        logger.info(f"Attempting to bind UDP socket to port {self.broadcast_port}...")
        self.broadcast_socket.bind(('', self.broadcast_port))
        logger.info(f"✓ UDP socket successfully bound to port {self.broadcast_port}")
        
    except PermissionError as e:
        logger.error(f"✗ Permission denied binding UDP socket to port {self.broadcast_port}: {e}")
        logger.error("Try running as root or using a port > 1024")
        raise
    except OSError as e:
        if e.errno == 98:  # Address already in use
            logger.error(f"✗ Port {self.broadcast_port} is already in use by another process")
            logger.error("Run 'sudo netstat -ulnp | grep {self.broadcast_port}' to see what's using it")
        else:
            logger.error(f"✗ OS error binding UDP socket to port {self.broadcast_port}: {e}")
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error setting up UDP socket: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        raise
    
    # Test TCP socket binding
    try:
        logger.info("Creating TCP sync socket...")
        self.sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        logger.info("✓ TCP socket created")
        
        logger.info("Setting TCP socket options...")
        self.sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        logger.info("✓ TCP socket options set")
        
        logger.info(f"Attempting to bind TCP socket to port {self.sync_port}...")
        self.sync_socket.bind(('', self.sync_port))
        logger.info(f"✓ TCP socket successfully bound to port {self.sync_port}")
        
        logger.info("Setting TCP socket to listen...")
        self.sync_socket.listen(5)
        logger.info("✓ TCP socket listening with backlog of 5")
        
    except PermissionError as e:
        logger.error(f"✗ Permission denied binding TCP socket to port {self.sync_port}: {e}")
        raise
    except OSError as e:
        if e.errno == 98:  # Address already in use
            logger.error(f"✗ Port {self.sync_port} is already in use by another process")
        else:
            logger.error(f"✗ OS error binding TCP socket to port {self.sync_port}: {e}")
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error setting up TCP socket: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        raise
    
    logger.info("✓ All network sockets successfully set up")

# To use this patch, replace the setup_sockets method in NetworkManager temporarily:
# 
# from KegDisplayDB.db.sync.network import NetworkManager
# NetworkManager.setup_sockets = improved_setup_sockets 