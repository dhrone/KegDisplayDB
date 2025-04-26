"""
Database module for KegDisplay providing synced database functionality.
"""

from .database import DatabaseManager
from .change_tracker import ChangeTracker
from .sync.synchronizer import DatabaseSynchronizer
from .sync.network import NetworkManager
from datetime import datetime, UTC
import json
import hashlib
import logging
import time
import sqlite3
import threading
import socket
import queue
import sys

logger = logging.getLogger(__name__)

class SyncedDatabase:
    """
    Main class that provides synchronized database functionality.
    This class integrates the database management, change tracking, and 
    synchronization components.
    """
    
    def __init__(self, db_path, broadcast_port=5002, sync_port=5003,
                 test_mode=False, notify_queue_size=100):
        # Core components
        self.db_manager = DatabaseManager(db_path, pool_size=1)
        self.change_tracker = ChangeTracker(self.db_manager)
        self.test_mode = test_mode
        self.test_peers = []
        if not self.test_mode:
            self.network = NetworkManager(broadcast_port, sync_port, self.change_tracker)
            self.synchronizer = DatabaseSynchronizer(
                self.db_manager, self.change_tracker, self.network
            )
        else:
            self.network = None
            self.synchronizer = None

        # Notification queue with throttling
        self._notify_queue = queue.Queue(maxsize=notify_queue_size)
        self._notify_stop = threading.Event()
        self._notify_worker = threading.Thread(
            target=self._notify_worker_loop,
            name='NotifyWorker',
            daemon=True
        )
        self._notify_worker.start()

    def _notify_worker_loop(self):
        while not self._notify_stop.is_set():
            try:
                peer, clock = self._notify_queue.get(timeout=1)
            except queue.Empty:
                continue
            try:
                if peer:
                    self.synchronizer.sync_now(peer=peer, clock=clock)
                else:
                    self.synchronizer.sync_now(clock=clock)
            except Exception as e:
                logger.error(f"Notify worker error: {e}")
            finally:
                self._notify_queue.task_done()


        
    def start(self):
        if not self.test_mode and self.synchronizer:
            self.synchronizer.start()

    def close(self):
        # Stop notify worker
        self._notify_stop.set()
        self._notify_worker.join(timeout=2)
        # Stop synchronizer
        if self.synchronizer:
            self.synchronizer.stop()
            self.synchronizer = None
        # Shutdown DBService
        try:
            if hasattr(self.db_manager, 'dbs') and self.db_manager.dbs:
                self.db_manager.dbs.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error shutting down DBService: {e}")
        finally:
            self.db_manager = None
            self.network = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
    
    def add_test_peer(self, peer):
        """Add a peer for test mode synchronization"""
        if self.test_mode and peer not in self.test_peers:
            self.test_peers.append(peer)
            peer.test_peers.append(self)
    
    # ---- Notification API ----
    def notify_update(self, clock=None):
        """Enqueue a sync notification or perform direct sync in test mode."""
        if self.test_mode:
            for peer_db in self.test_peers:
                if peer_db is not self and peer_db.synchronizer:
                    peer_db.synchronizer.sync_now(peer=peer_db.synchronizer, clock=clock)
            return
        # Production: enqueue, drop if queue full
        try:
            self._notify_queue.put((None, clock), block=False)
        except queue.Full:
            logger.warning("Notify queue full; dropping update notification")

    def sync_now(self, peer=None, clock=None):
        """Public sync API: unify production broadcast and test-mode peer sync.

        Args:
            peer: Optional peer DatabaseSynchronizer (for direct sync in tests)
            clock: Optional logical clock value to use (forwarded to notify)
        """
        if peer is not None:
            # Test-mode path: sync directly with this peer's state
            self._sync_with_peer(peer)
        else:
            # Production path: broadcast to all peers
            self.notify_update(clock)
    
    def _notify_update_with_timeout(self, clock=None):
        """Execute the notification with timeout protection"""
        try:
            # Use synchronizer to broadcast update
            self.synchronizer.notify_update(clock)
        except socket.error as e:
            logger.error(f"Network error during notification: {e}")
        except Exception as e:
            logger.error(f"Error in notification thread: {e}")
    
    def add_peer(self, peer_ip):
        """Manually add a peer by IP address"""
        if not self.test_mode:
            self.synchronizer.add_peer(peer_ip)
    
    def stop(self):
        """Stop the sync service"""
        self.close()
    
    # ---- Beer Management Methods ----
    
    def add_beer(self, name, abv=None, ibu=None, color=None, og=None, fg=None,
                 description=None, brewed=None, kegged=None, tapped=None,
                 notes=None, notify=True):
        beer_id = self.db_manager.add_beer(
            name, abv, ibu, color, og, fg,
            description, brewed, kegged, tapped, notes
        )
        clock = self.change_tracker.log_change("beers", "INSERT", beer_id)
        if notify:
            self.notify_update(clock)
        return beer_id

    def update_beer(self, beer_id, name=None, abv=None, ibu=None, color=None, og=None, fg=None,
                 description=None, brewed=None, kegged=None, tapped=None,
                 notes=None, notify=True):
        success = self.db_manager.update_beer(beer_id, name, abv, ibu, color, og, fg, description, brewed, kegged, tapped, notes)
        if not success:
            return False
        clock = self.change_tracker.log_change("beers", "UPDATE", beer_id, increment_clock=True)
        if notify:
            self.notify_update(clock)
        return True

    def delete_beer(self, beer_id, notify=True):
        tap_ids = self.db_manager.get_tap_with_beer(beer_id)
        success = self.db_manager.delete_beer(beer_id)
        if not success:
            return False
        clock = self.change_tracker.log_change("beers", "DELETE", beer_id)
        for t in tap_ids:
            self.db_manager.update_tap(t, None)
            self.change_tracker.log_change("taps", "UPDATE", t, increment_clock=False)
        if notify:
            self.notify_update(clock)
        return True
    
    def get_beer(self, beer_id):
        """
        Get a beer by ID
        
        Args:
            beer_id: ID of the beer to retrieve
            
        Returns:
            Beer data dictionary or None if not found
        """
        try:
            return self.db_manager.get_beer(beer_id)
        except Exception as e:
            logger.error(f"Error getting beer {beer_id}: {e}")
            return None
    
    def get_all_beers(self):
        """
        Get all beers from the database
        
        Returns:
            List of beer dictionaries
        """
        try:
            return self.db_manager.get_all_beers()
        except Exception as e:
            logger.error(f"Error retrieving all beers: {e}")
            return []
    
    # ---- Tap Management Methods ----
    
    def add_tap(self, tap_id=None, beer_id=None, notify=True):
        new_tap = self.db_manager.add_tap(tap_id, beer_id)
        clock = self.change_tracker.log_change("taps", "INSERT", new_tap)
        if notify:
            self.notify_update(clock)
        return new_tap

    def update_tap(self, tap_id, beer_id, notify=True):
        success = self.db_manager.update_tap(tap_id, beer_id)
        if not success:
            return False
        clock = self.change_tracker.log_change("taps", "UPDATE", tap_id, increment_clock=True)
        if notify:
            self.notify_update(clock)
        return True

    def delete_tap(self, tap_id, notify=True):
        success = self.db_manager.delete_tap(tap_id)
        if not success:
            return False
        clock = self.change_tracker.log_change("taps", "DELETE", tap_id, increment_clock=True)
        if notify:
            self.notify_update(clock)
        return True
    
    def get_tap(self, tap_id):
        """
        Get a tap by ID
        
        Args:
            tap_id: ID of the tap to retrieve
            
        Returns:
            Tap data dictionary or None if not found
        """
        try:
            return self.db_manager.get_tap(tap_id)
        except Exception as e:
            logger.error(f"Error retrieving tap {tap_id}: {e}")
            return None
    
    def get_all_taps(self):
        """
        Get all taps with their beer information
        
        Returns:
            List of tap dictionaries
        """
        try:
            return self.db_manager.get_all_taps()
        except Exception as e:
            logger.error(f"Error retrieving all taps: {e}")
            return []
    
    def get_tap_with_beer(self, beer_id):
        """
        Find taps that have a specific beer
        
        Args:
            beer_id: ID of the beer to find in taps
            
        Returns:
            List of tap IDs that have the specified beer
        """
        try:
            return self.db_manager.get_tap_with_beer(beer_id)
        except Exception as e:
            logger.error(f"Error finding taps with beer {beer_id}: {e}")
            return []
    
    # ---- Bulk Operations ----
    
    def import_beers_from_data(self, beer_data_list):
        """
        Import multiple beers from a list of dictionaries
        
        Args:
            beer_data_list: List of beer dictionaries with all beer fields
            
        Returns:
            Tuple of (success_count, errors)
        """
        logger.info(f"Importing {len(beer_data_list)} beers")
        
        # Deduplicate and sort the beer data by ID to ensure consistent order
        unique_beers = {}
        for beer in beer_data_list:
            beer_id = beer.get('idBeer')
            if beer_id:
                try:
                    beer_id = int(beer_id)
                    unique_beers[beer_id] = beer
                except (ValueError, TypeError):
                    # If ID can't be converted to int, use the beer name as key
                    unique_beers[beer.get('Name', str(id(beer)))] = beer
            else:
                # Use the beer name as key if no ID
                unique_beers[beer.get('Name', str(id(beer)))] = beer
        
        # Sort by ID (numeric keys will be sorted first)
        sorted_beers = []
        for key in sorted(unique_beers.keys()):
            sorted_beers.append(unique_beers[key])
            
        logger.info(f"Processing {len(sorted_beers)} unique beers")
        
        # Initialize counters and collections
        success_count = 0
        errors = []
        
        # Define batch size
        BATCH_SIZE = 100
        
        # Start a transaction for the entire import
        try:
            # Clear all existing beer related data
            try:
                self.db_manager.execute("DELETE FROM beers")
                self.db_manager.execute("DELETE FROM taps")
                self.db_manager.execute("DELETE FROM change_log")
            except Exception as e:
                logger.error(f"Error clearing beer related data: {e}")
                return (0, ["Failed to clear beer related data"])
    
            #clock = self.change_tracker.log_change("version", "CLEAR", 1)  # Disable clear logging for now
            first_call = True
            # Process beers in batches
            for batch_start in range(0, len(sorted_beers), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(sorted_beers))
                batch = sorted_beers[batch_start:batch_end]
                
                logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1} with {len(batch)} beers")
                
                # Process each beer in the batch
                batch_success_count = 0
                for idx, beer_data in enumerate(batch):
                    try:
                        # Add beer using db_manager (all operations are inserts)
                        beer_id = self.db_manager.add_beer(
                            name=beer_data.get('Name'),
                            abv=beer_data.get('ABV'),
                            ibu=beer_data.get('IBU'),
                            color=beer_data.get('Color'),
                            og=beer_data.get('OriginalGravity'),
                            fg=beer_data.get('FinalGravity'),
                            description=beer_data.get('Description'),
                            brewed=beer_data.get('Brewed'),
                            kegged=beer_data.get('Kegged'),
                            tapped=beer_data.get('Tapped'),
                            notes=beer_data.get('Notes')
                        )
                        clock = self.change_tracker.log_change("beers", "INSERT", beer_id, increment_clock=first_call)
                        first_call = False
                        
                        if beer_id:
                            batch_success_count += 1
                            logger.info(f"Added beer '{beer_data.get('Name')}' with ID {beer_id}")
                        
                    except Exception as e:
                        logger.error(f"Error processing beer {batch_start + idx + 1}: {str(e)}")
                        errors.append(f"Error on beer {batch_start + idx + 1}: {str(e)}")
                
                # Update total success count
                success_count += batch_success_count
                
            # Even if no records were added, we still need to notify peers about the clear
            self.notify_update(clock)
            
            logger.info(f"Successfully imported {success_count} beers")
            
        except Exception as e:
            # Handle any unexpected errors
            logger.error(f"Error during beer import: {str(e)}")
            errors.append(f"Transaction error: {str(e)}")
        
        return (success_count, errors)
    
    
    def clear_all_beers(self):
        """
        Remove all data from the database.  This includes all beers, taps, and change log.
        
        Returns:
            Number of beers cleared
        """
        try:
            # Get all beers first to count them
            beers = self.get_all_beers()
            beer_count = len(beers)
            
            if beer_count > 0:
                # Clear all taps first to avoid foreign key issues
                self.db_manager.clear_tap()
                
                # Then clear all beers
                self.db_manager.clear_beer()

                # Clear the change log
                self.db_manager.clear_change_log()

                return beer_count
                
            return True
        except Exception as e:
            logger.error(f"Error clearing beers")
            raise
    
    def set_tap_count(self, count):
        """
        Set the number of taps in the system
        
        Args:
            count: Desired number of taps (positive integer)
            
        Returns:
            Boolean indicating success
        """
        if not isinstance(count, int) or count < 1:
            return False
            
        try:
            # Get current taps
            existing_taps = self.get_all_taps()
            current_count = len(existing_taps)
            clock = None
            increment_clock = True
            # If decreasing, delete excess taps
            if count < current_count:
                # Delete
                #  taps from highest number to lowest
                for i in range(current_count, count, -1):
                    tap_id = i
                    success = self.db_manager.delete_tap(tap_id)
                    if success:
                        clock = self.change_tracker.log_change("taps", "DELETE", tap_id, increment_clock=increment_clock) if not clock else clock
                        increment_clock = False

            
            # If increasing, add new taps
            elif count > current_count:
                # Add new taps with sequential IDs
                for i in range(current_count + 1, count + 1):
                    tap_id = i
                    success = self.db_manager.add_tap(tap_id, None)
                    if success:
                        clock = self.change_tracker.log_change("taps", "INSERT", tap_id, increment_clock=increment_clock) if not clock else clock
                        increment_clock = False
            
            # Send a single notification after all changes
            self.notify_update(clock)
            return True
            
        except Exception as e:
            logger.error(f"Error setting tap count: {e}")
            return False 