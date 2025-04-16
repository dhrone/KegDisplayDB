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
        
        # Initialize SyncedDatabase
        self.db = None
        self.db_manager = None
        
    def start(self):
        """Start the database sync service"""
        if self.running:
            logger.warning("Service is already running")
            return
            
        logger.info("Starting database sync service in client mode")
        
        try:
            # Initialize the database and managers
            self.db_manager = DatabaseManager(self.db_path)
            
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
            
            # Print some status information
            self._print_status()
            
            # Keep the service running until exit is requested
            while not self.exit_requested:
                time.sleep(5)  # Check for exit every 5 seconds
                
        except Exception as e:
            logger.error(f"Error in sync service: {e}", exc_info=True)
            self.stop()
    
    def _print_status(self):
        """Print status information about the database"""
        try:
            beers_count = len(self.db.get_all_beers())
            taps_count = len(self.db.get_all_taps())
            logger.info(f"Database contains {beers_count} beers and {taps_count} taps")
        except Exception as e:
            logger.error(f"Error getting database status: {e}")
    
    def stop(self):
        """Stop the database sync service"""
        logger.info("Stopping database sync service")
        self.exit_requested = True
        
        if self.db:
            self.db.stop()
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
    service = DBSyncService(
        db_path=args.db_path,
        primary_ip=args.primary_ip,
        broadcast_port=args.broadcast_port,
        sync_port=args.sync_port
    )
    
    service.start()

if __name__ == "__main__":
    service = None
    main()
