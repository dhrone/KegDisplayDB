#!/usr/bin/env python3
"""
Database Synchronization Service for KegDisplay

This module provides a standalone service that handles database synchronization
between multiple KegDisplay instances. It has been updated to use the same
synchronization methods as the web interface.

Usage:
  python -m KegDisplayDB.dbsync.service [--primary-ip <ip>]
"""

import os
import sys
import time
import logging
import argparse
import threading
import signal
import json
from pathlib import Path

# Import log configuration
from ..utils.log_config import configure_logging

# Import SyncedDatabase and DatabaseManager
from ..db import SyncedDatabase
from ..db.database import DatabaseManager

# Get the pre-configured logger
logger = logging.getLogger("KegDisplay.DBSync")

# Define default paths
USER_HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(USER_HOME, ".KegDisplayDB")
DATA_DIR = os.path.join(CONFIG_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, 'beer.db')

class DBSyncService:
    """
    Database synchronization service in client mode.
    
    This service uses the SyncedDatabase class to handle synchronization between
    different instances of KegDisplayDB, without the web interface components.
    """
    
    def __init__(self, db_path=None, primary_ip=None, broadcast_port=5002, sync_port=5003):
        """Initialize the database sync service
        
        Args:
            db_path: Path to the database file
            primary_ip: IP address of the primary server (optional)
            broadcast_port: Port for UDP broadcast
            sync_port: Port for TCP sync connections
        """
        self.running = False
        self.exit_requested = False
        self.exit_event = threading.Event()
        
        # Set default database path if not provided
        if db_path is None:
            # Ensure the data directory exists
            os.makedirs(DATA_DIR, exist_ok=True)
            self.db_path = DB_PATH
        else:
            self.db_path = db_path
            
        logger.info(f"Database path: {self.db_path}")
        
        # Store other parameters
        self.primary_ip = primary_ip
        self.broadcast_port = broadcast_port
        self.sync_port = sync_port
        
        # Validate ports
        if not isinstance(broadcast_port, int) or broadcast_port <= 0 or broadcast_port > 65535:
            raise ValueError(f"Invalid broadcast port: {broadcast_port}")
        if not isinstance(sync_port, int) or sync_port <= 0 or sync_port > 65535:
            raise ValueError(f"Invalid sync port: {sync_port}")
        
        # Initialize SyncedDatabase
        self.db = None
        
        # Initialize status monitoring thread
        self.status_thread = None
        
    def start(self):
        """Start the database sync service"""
        if self.running:
            logger.warning("Service is already running")
            return
            
        logger.info("Starting database sync service")
        
        try:
            # Initialize the SyncedDatabase instance for synchronization
            self.db = SyncedDatabase(
                db_path=self.db_path,
                broadcast_port=self.broadcast_port,
                sync_port=self.sync_port,
                test_mode=False
            )
            
            # If a primary server IP is specified, add it as a peer
            if self.primary_ip:
                logger.info(f"Adding primary server as peer: {self.primary_ip}")
                self.db.add_peer(self.primary_ip)
            
            self.running = True
            
            # Print initial status information
            self._print_status()
            
            # Start status monitoring thread
            self.exit_event.clear()
            self.status_thread = threading.Thread(target=self._status_monitor, daemon=True)
            self.status_thread.start()
            
            # Keep the service running until exit is requested
            logger.info("Service started successfully")
            while not self.exit_requested and not self.exit_event.is_set():
                self.exit_event.wait(5)  # Wait for exit event with timeout
                
        except Exception as e:
            logger.error(f"Error in sync service: {e}", exc_info=True)
            self.stop()
    
    def _status_monitor(self):
        """Periodically monitor and report service status"""
        while self.running and not self.exit_event.is_set():
            try:
                self._print_status()
            except Exception as e:
                logger.error(f"Error in status monitoring: {e}")
            
            # Wait for 60 seconds or until exit is requested
            if self.exit_event.wait(60):
                break
    
    def _print_status(self):
        """Print status information about the database"""
        try:
            if not self.db:
                logger.warning("No database connection available")
                return
                
            beers_count = len(self.db.get_all_beers())
            taps_count = len(self.db.get_all_taps())
            peers = self.db.get_peers() if hasattr(self.db, 'get_peers') else []
            
            logger.info(f"Database contains {beers_count} beers and {taps_count} taps")
            logger.info(f"Connected peers: {len(peers)}")
            
            # Add more detailed status information if available
            if hasattr(self.db, 'get_sync_status'):
                sync_status = self.db.get_sync_status()
                logger.info(f"Sync status: {sync_status}")
        except Exception as e:
            logger.error(f"Error getting database status: {e}")
    
    def stop(self):
        """Stop the database sync service"""
        logger.info("Stopping database sync service")
        self.exit_requested = True
        self.exit_event.set()
        
        # Gracefully stop the status thread if it's running
        if self.status_thread and self.status_thread.is_alive():
            try:
                self.status_thread.join(timeout=2)
            except Exception as e:
                logger.warning(f"Error stopping status thread: {e}")
        
        # Gracefully stop the database connection
        if self.db:
            try:
                self.db.stop()
            except Exception as e:
                logger.error(f"Error stopping SyncedDatabase: {e}")
            finally:
                self.db = None
            
        self.running = False
        logger.info("Database sync service stopped")

def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {sig}, shutting down...")
    if service:
        service.stop()
    sys.exit(0)

def main():
    """Main entry point for the service"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='KegDisplay Database Sync Service (Client Mode)')
    parser.add_argument('--db-path',
                       help='Path to SQLite database file')
    parser.add_argument('--primary-ip',
                       help='IP address of the primary server (optional, will use broadcast discovery if not provided)')
    parser.add_argument('--broadcast-port',
                       type=int, 
                       default=5002,
                       help='Port for broadcast messages')
    parser.add_argument('--sync-port',
                       type=int,
                       default=5003,
                       help='Port for sync connections')
    parser.add_argument('--log-level', 
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       type=str.upper,
                       help='Set the logging level')
    
    args = parser.parse_args()
    
    # Configure logging with the specified level
    configure_logging(args.log_level)
    logger.debug(f"Starting dbsync service with log level {args.log_level}")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start the service
    global service
    try:
        service = DBSyncService(
            db_path=args.db_path,
            primary_ip=args.primary_ip,
            broadcast_port=args.broadcast_port,
            sync_port=args.sync_port
        )
        
        service.start()
    except Exception as e:
        logger.critical(f"Failed to start service: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    service = None
    main()
