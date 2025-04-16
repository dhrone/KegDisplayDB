#!/usr/bin/env python3
"""
Database Synchronization Service for KegDisplay

This module provides a standalone service that handles database synchronization
between multiple KegDisplay instances. It can run in either primary mode (with webinterface)
or client mode (receiving updates only).

Usage:
  python -m KegDisplay.dbsync_service --mode client
  python -m KegDisplay.dbsync_service --mode primary
"""

import os
import sys
import time
import logging
import argparse
import threading
import signal
from pathlib import Path

# Import log configuration
from .log_config import configure_logging

# Import SyncedDatabase from db package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from KegDisplay.db import SyncedDatabase

# Get the pre-configured logger
logger = logging.getLogger("KegDisplay.DBSync")

class DBSyncService:
    """
    Database synchronization service that can run in either primary or client mode.
    
    In primary mode, it's designed to run alongside the webinterface.
    In client mode, it only listens for updates from the primary.
    """
    
    def __init__(self, mode='client', db_path=None, primary_ip=None, broadcast_port=5002, sync_port=5003):
        """Initialize the database sync service
        
        Args:
            mode: 'primary' or 'client'
            db_path: Path to the database file
            primary_ip: IP address of the primary server (for client mode)
            broadcast_port: Port for UDP broadcast
            sync_port: Port for sync connections
        """
        self.mode = mode
        self.running = False
        self.exit_requested = False
        
        # Set default database path if not provided
        if db_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, 'beer.db')
        else:
            self.db_path = db_path
            
        logger.info(f"Database path: {self.db_path}")
        
        # Store other parameters
        self.primary_ip = primary_ip
        self.broadcast_port = broadcast_port
        self.sync_port = sync_port
        
        # Initialize SyncedDatabase
        self.db = None
        
    def start(self):
        """Start the database sync service"""
        if self.running:
            logger.warning("Service is already running")
            return
            
        logger.info(f"Starting database sync service in {self.mode} mode")
        
        try:
            # Initialize the database sync instance
            self.db = SyncedDatabase(
                db_path=self.db_path,
                broadcast_port=self.broadcast_port,
                sync_port=self.sync_port,
                test_mode=False
            )
            
            # For clients with known primary server, add it as a peer
            if self.mode == 'client' and self.primary_ip:
                logger.info(f"Adding primary server as peer: {self.primary_ip}")
                self.db.add_peer(self.primary_ip)
            
            self.running = True
            
            # If in primary mode, we would typically integrate with the webinterface
            # to detect changes and notify peers. That logic happens in the webinterface.py.
            
            # Keep the service running until exit is requested
            while not self.exit_requested:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in sync service: {e}")
            self.stop()
    
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

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='KegDisplay Database Sync Service')
    parser.add_argument('--mode', 
                       choices=['primary', 'client'],
                       default='client',
                       help='Operation mode: primary (with webinterface) or client')
    parser.add_argument('--db-path',
                       help='Path to SQLite database file')
    parser.add_argument('--primary-ip',
                       help='IP address of the primary server (client mode only)')
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
                       help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    
    args = parser.parse_args()
    
    # Configure logging with the specified level
    configure_logging(args.log_level)
    logger.debug(f"Starting dbsync service with log level {args.log_level}")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start the service
    service = DBSyncService(
        mode=args.mode,
        db_path=args.db_path,
        primary_ip=args.primary_ip,
        broadcast_port=args.broadcast_port,
        sync_port=args.sync_port
    )
    
    service.start() 