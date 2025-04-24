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

logger = logging.getLogger(__name__)

class SyncedDatabase:
    """
    Main class that provides synchronized database functionality.
    This class integrates the database management, change tracking, and 
    synchronization components.
    """
    
    def __init__(self, db_path, broadcast_port=5002, sync_port=5003, test_mode=False):
        """Initialize a SyncedDatabase instance
        
        Args:
            db_path: Path to the SQLite database
            broadcast_port: Port for UDP broadcast messages
            sync_port: Port for TCP sync connections
            test_mode: Whether to operate in test mode (bypassing actual network operations)
        """
        # Using a minimal connection pool size of 1 to prevent SQLite database locking issues.
        # SQLite has limitations when multiple connections try to write simultaneously,
        # and larger connection pools can lead to "database is locked" errors.
        self.db_manager = DatabaseManager(db_path, pool_size=1)  # Reduced pool size to minimize lock contention
        self.change_tracker = ChangeTracker(self.db_manager)
        self.test_mode = test_mode
        self.test_peers = []
        
        if not test_mode:
            self.network = NetworkManager(broadcast_port, sync_port, self.change_tracker)
            self.synchronizer = DatabaseSynchronizer(
                self.db_manager, 
                self.change_tracker, 
                self.network
            )
            self.synchronizer.start()
        else:
            # Initialize synchronizer with mock network for test mode
            # The actual NetworkManager will be mocked in tests
            self.synchronizer = None
    
    def __del__(self):
        """Cleanup resources when the instance is being garbage collected"""
        self.close()
        
    def close(self):
        """Close database connections and cleanup resources"""
        try:
            # Stop the synchronizer if it exists
            if not self.test_mode and hasattr(self, 'synchronizer') and self.synchronizer:
                self.synchronizer.stop()
                self.synchronizer = None
                
            # Close database manager if it exists
            if hasattr(self, 'db_manager') and self.db_manager:
                # Remove reference to allow garbage collection
                db_manager = self.db_manager
                self.db_manager = None
                
                # Let garbage collector handle the rest
        except Exception as e:
            logger.error(f"Error in SyncedDatabase.close(): {e}")
    
    def add_test_peer(self, peer):
        """Add a peer for test mode synchronization"""
        if self.test_mode and peer not in self.test_peers:
            self.test_peers.append(peer)
            peer.test_peers.append(self)
    
    def notify_update(self, clock=None):
        """Notify other instances that a change has been made"""
        try:
            if self.test_mode:
                # In test mode, directly sync with test peers
                for peer in self.test_peers:
                    try:
                        if peer != self and hasattr(peer, 'synchronizer') and peer.synchronizer:
                            # Only sync with peer if both have synchronizers
                            if hasattr(self, 'synchronizer') and self.synchronizer:
                                self.synchronizer._sync_with_peer(peer, clock)
                    except Exception as e:
                        logger.error(f"Error syncing with test peer: {e}")
            else:
                # Use synchronizer to broadcast update if available
                if hasattr(self, 'synchronizer') and self.synchronizer:
                    try:
                        # Give a short timeout for network operations
                        notify_thread = threading.Thread(
                            target=self._notify_update_with_timeout,
                            kwargs={'clock': clock},
                            daemon=True
                        )
                        notify_thread.start()
                        notify_thread.join(timeout=2.0)  # Wait up to 2 seconds
                        
                        # Log success
                        logger.info("Update notification broadcast complete or timed out")
                    except Exception as e:
                        logger.error(f"Error starting notification thread: {e}")
                else:
                    logger.warning("Notification skipped: no synchronizer available")
        except Exception as e:
            logger.error(f"Error in notify_update: {e}")
    
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
                description=None, brewed=None, kegged=None, tapped=None, notes=None, notify=True):
        """
        Add a new beer to the database
        
        Args:
            name: Beer name (required)
            abv: Alcohol by volume (optional)
            ibu: International bitterness units (optional)
            color: Beer color (optional)
            og: Original gravity (optional)
            fg: Final gravity (optional)
            description: Beer description (optional)
            brewed: Date brewed (optional)
            kegged: Date kegged (optional)
            tapped: Date tapped (optional)
            notes: Additional notes (optional)
            notify: Whether to notify peers about this change (default: True)
            conn: Database connection to use (optional)
            
        Returns:
            beer_id: ID of the added beer
        """

        try:
            beer_id = self.db_manager.add_beer(name, abv, ibu, color, og, fg, 
                                                description, brewed, kegged, tapped, notes)
                                      
            clock = self.change_tracker.log_change("beers", "INSERT", beer_id)
            
            if notify:
                self.notify_update(clock)
            
            return beer_id
        
        except Exception as e:
            logger.error(f"Error adding beer: {e}")
            return None
    
    def update_beer(self, beer_id, name=None, abv=None, ibu=None, color=None, og=None, fg=None,
                   description=None, brewed=None, kegged=None, tapped=None, notes=None, notify=True):
        """
        Update an existing beer in the database
        
        Args:
            beer_id: ID of the beer to update
            name: Beer name (optional)
            abv: Alcohol by volume (optional)
            ibu: International bitterness units (optional)
            color: Beer color (optional)
            og: Original gravity (optional)
            fg: Final gravity (optional)
            description: Beer description (optional)
            brewed: Date brewed (optional)
            kegged: Date kegged (optional)
            tapped: Date tapped (optional)
            notes: Additional notes (optional)
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            bool: Success or failure
        """
        try:
            success = self.db_manager.update_beer(beer_id, name, abv, ibu, color, 
                                                  og, fg, description, brewed, 
                                                  kegged, tapped, notes)
                                                    
            if success:
                clock = self.change_tracker.log_change("beers", "UPDATE", beer_id)
                
                if notify:
                    self.notify_update(clock)
                    
            return success
        except Exception as e:
            logger.error(f"Error updating beer {beer_id}: {e}")
            return False
    
    def delete_beer(self, beer_id, notify=True):
        """
        Delete a beer from the database
        
        Args:
            beer_id: ID of the beer to delete
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            bool: Success or failure
        """
        try:
            # Get taps with this beer before deleting (so we can update them)
            tap_ids = self.db_manager.get_tap_with_beer(beer_id)
            
            # Delete the beer
            success = self.db_manager.delete_beer(beer_id)
            if success:
                clock = self.change_tracker.log_change("beers", "DELETE", beer_id)

                # Update any taps that had this beer to have None
                if tap_ids:
                    for tap_id in tap_ids:
                        update_success = self.db_manager.update_tap(tap_id, None)
                        if update_success:
                            self.change_tracker.log_change("taps", "UPDATE", tap_id, increment_clock=False)
                
                if notify:
                    self.notify_update(clock)
                        
                return success
        except Exception as e:
            logger.error(f"Error deleting beer {beer_id}: {e}")
            return False
    
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
        """
        Add a new tap to the database
        
        Args:
            tap_id: Optional tap ID (auto-assigned if not provided)
            beer_id: Optional beer ID to assign to this tap
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            int: ID of the new tap or None if failed
        """
        try:
            tap_id = self.db_manager.add_tap(tap_id, beer_id)
            if tap_id:
                clock = self.change_tracker.log_change("taps", "INSERT", tap_id)
                
                if notify:
                    self.notify_update(clock)
                    
            return tap_id
        except Exception as e:
            logger.error(f"Error adding tap: {e}")
            return None
    
    def update_tap(self, tap_id, beer_id, notify=True):
        """
        Update a tap's beer assignment
        
        Args:
            tap_id: ID of the tap to update
            beer_id: ID of beer to assign (or None to clear)
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            bool: Success or failure
        """
        try:
            success = self.db_manager.update_tap(tap_id, beer_id)
            if success:
                clock = self.change_tracker.log_change("taps", "UPDATE", tap_id)
                
                if notify:
                    self.notify_update(clock)
                    
            return success
        except Exception as e:
            logger.error(f"Error updating tap {tap_id}: {e}")
            return False
    
    def delete_tap(self, tap_id, notify=True):
        """
        Delete a tap from the database
        
        Args:
            tap_id: ID of the tap to delete
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            bool: Success or failure
        """
        try:
            success = self.db_manager.delete_tap(tap_id)
            if success:
                clock = self.change_tracker.log_change("taps", "DELETE", tap_id)
                
                if notify:
                    self.notify_update(clock)
                    
            return success
        except Exception as e:
            logger.error(f"Error deleting tap {tap_id}: {e}")
            return False
    
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
                self.db_manager.execute("BEGIN TRANSACTION; DELETE FROM beers; DELETE FROM taps; COMMIT;")
            except Exception as e:
                logger.error(f"Error clearing beer related data: {e}")
                return (0, ["Failed to clear beer related data"])
    
            clock = self.change_tracker.log_change("version", "CLEAR", 1)
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
                        self.change_tracker.log_change("beers", "INSERT", beer_id, increment_clock=False)
                        
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
        Remove all beers from the database
        
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
                
                # Log the change
                clock = self.change_tracker.log_change("version", "CLEAR", 1)
                
                # Send notification after transaction is committed
                self.notify_update(clock)
                return beer_count
                
            return beer_count
        except Exception as e:
            logger.error(f"Error clearing beers: {e}")
            return 0
    
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
                    success = self.db_manager.add_tap(tap_id, beer_id)
                    if success:
                        clock = self.change_tracker.log_change("taps", "INSERT", tap_id, increment_clock=increment_clock) if not clock else clock
                        increment_clock = False
            
            # Send a single notification after all changes
            self.notify_update(clock)
            return True
            
        except Exception as e:
            logger.error(f"Error setting tap count: {e}")
            return False 