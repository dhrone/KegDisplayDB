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
        self.db_manager = DatabaseManager(db_path)
        self.change_tracker = ChangeTracker(self.db_manager)
        self.test_mode = test_mode
        self.test_peers = []
        
        if not test_mode:
            self.network = NetworkManager(broadcast_port, sync_port)
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
    
    def add_test_peer(self, peer):
        """Add a peer for test mode synchronization"""
        if self.test_mode and peer not in self.test_peers:
            self.test_peers.append(peer)
            peer.test_peers.append(self)
    
    def notify_update(self):
        """Notify other instances that a change has been made"""
        try:
            if self.test_mode:
                # In test mode, directly sync with test peers
                for peer in self.test_peers:
                    try:
                        if peer != self and hasattr(peer, 'synchronizer') and peer.synchronizer:
                            # Only sync with peer if both have synchronizers
                            if hasattr(self, 'synchronizer') and self.synchronizer:
                                self.synchronizer._sync_with_peer(peer)
                    except Exception as e:
                        logger.error(f"Error syncing with test peer: {e}")
            else:
                # Use synchronizer to broadcast update if available
                if hasattr(self, 'synchronizer') and self.synchronizer:
                    try:
                        # Give a short timeout for network operations
                        notify_thread = threading.Thread(
                            target=self._notify_update_with_timeout,
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
    
    def _notify_update_with_timeout(self):
        """Execute the notification with timeout protection"""
        try:
            # Use synchronizer to broadcast update
            self.synchronizer.notify_update()
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
        if not self.test_mode and hasattr(self, 'synchronizer'):
            self.synchronizer.stop()
    
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
            
        Returns:
            beer_id: ID of the added beer
        """
        beer_id = self.db_manager.add_beer(name, abv, ibu, color, og, fg, 
                                         description, brewed, kegged, tapped, notes)
        self.change_tracker.log_change("beers", "INSERT", beer_id)
        
        if notify:
            self.notify_update()
            
        return beer_id
    
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
        success = self.db_manager.update_beer(beer_id, name, abv, ibu, color, 
                                            og, fg, description, brewed, 
                                            kegged, tapped, notes)
        if success:
            self.change_tracker.log_change("beers", "UPDATE", beer_id)
            
            if notify:
                self.notify_update()
                
        return success
    
    def delete_beer(self, beer_id, notify=True):
        """
        Delete a beer from the database
        
        Args:
            beer_id: ID of the beer to delete
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            bool: Success or failure
        """
        # First check for taps using this beer
        taps_with_beer = self.get_tap_with_beer(beer_id)
        
        # Update those taps first
        for tap_id in taps_with_beer:
            self.update_tap(tap_id, None, notify=False)
        
        # Now delete the beer
        success = self.db_manager.delete_beer(beer_id)
        if success:
            self.change_tracker.log_change("beers", "DELETE", beer_id)
            
            if notify:
                self.notify_update()
                
        return success
    
    def get_beer(self, beer_id):
        """Get a beer by ID"""
        return self.db_manager.get_beer(beer_id)
    
    def get_all_beers(self):
        """Get all beers from the database"""
        return self.db_manager.get_all_beers()
    
    # ---- Tap Management Methods ----
    
    def add_tap(self, tap_id=None, beer_id=None, notify=True):
        """
        Add a new tap to the database
        
        Args:
            tap_id: Optional tap ID (auto-assigned if not provided)
            beer_id: Optional beer ID to assign to this tap
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            int: ID of the new tap
        """
        tap_id = self.db_manager.add_tap(tap_id, beer_id)
        if tap_id:
            self.change_tracker.log_change("taps", "INSERT", tap_id)
            
            if notify:
                self.notify_update()
                
        return tap_id
    
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
        success = self.db_manager.update_tap(tap_id, beer_id)
        if success:
            self.change_tracker.log_change("taps", "UPDATE", tap_id)
            
            if notify:
                self.notify_update()
                
        return success
    
    def delete_tap(self, tap_id, notify=True):
        """
        Delete a tap from the database
        
        Args:
            tap_id: ID of the tap to delete
            notify: Whether to notify peers about this change (default: True)
            
        Returns:
            bool: Success or failure
        """
        success = self.db_manager.delete_tap(tap_id)
        if success:
            self.change_tracker.log_change("taps", "DELETE", tap_id)
            
            if notify:
                self.notify_update()
                
        return success
    
    def get_tap(self, tap_id):
        """Get a tap by ID"""
        return self.db_manager.get_tap(tap_id)
    
    def get_all_taps(self):
        """Get all taps with their beer information"""
        return self.db_manager.get_all_taps()
    
    def get_tap_with_beer(self, beer_id):
        """Find taps that have a specific beer"""
        return self.db_manager.get_tap_with_beer(beer_id)
    
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
        
        # Initialize counters and collections
        success_count = 0
        errors = []
        changes_to_log = []
        current_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Define batch size to prevent long transactions
        BATCH_SIZE = 50
        
        # Process in batches
        for batch_start in range(0, len(beer_data_list), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(beer_data_list))
            batch = beer_data_list[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1} with {len(batch)} beers")
            
            # Use a connection for this batch
            with self.db_manager.get_connection() as conn:
                try:
                    batch_success_count = 0
                    batch_changes = []
                    
                    # Process each beer in the batch
                    for idx, beer_data in enumerate(batch):
                        try:
                            # Handle existing beer with same ID
                            beer_id = beer_data.get('idBeer')
                            existing_beer = None
                            
                            if beer_id and str(beer_id).strip() and str(beer_id) != '':
                                try:
                                    beer_id = int(beer_id)
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
                                    existing_beer = cursor.fetchone()
                                except (ValueError, TypeError):
                                    beer_id = None
                            
                            # Update existing beer or add new one
                            if existing_beer:
                                # Update the beer
                                cursor = conn.cursor()
                                cursor.execute('''
                                    UPDATE beers SET
                                        Name = ?, ABV = ?, IBU = ?, Color = ?, OriginalGravity = ?, FinalGravity = ?,
                                        Description = ?, Brewed = ?, Kegged = ?, Tapped = ?, Notes = ?
                                    WHERE idBeer = ?
                                ''', (
                                    beer_data.get('Name'),
                                    beer_data.get('ABV'),
                                    beer_data.get('IBU'),
                                    beer_data.get('Color'),
                                    beer_data.get('OriginalGravity'),
                                    beer_data.get('FinalGravity'),
                                    beer_data.get('Description'),
                                    beer_data.get('Brewed'),
                                    beer_data.get('Kegged'),
                                    beer_data.get('Tapped'),
                                    beer_data.get('Notes'),
                                    beer_id
                                ))
                                
                                # Get content for the row for change tracking
                                content = self.change_tracker._get_row_content("beers", beer_id, conn)
                                content_hash = hashlib.md5(content.encode()).hexdigest()
                                
                                # Store change to log later
                                batch_changes.append({
                                    "table_name": "beers",
                                    "operation": "UPDATE",
                                    "row_id": beer_id,
                                    "content": content,
                                    "content_hash": content_hash
                                })
                                batch_success_count += 1
                                logger.info(f"Updated beer '{beer_data.get('Name')}' with ID {beer_id}")
                            else:
                                # Add new beer
                                beer_id = self.add_beer(
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
                                    notes=beer_data.get('Notes'),
                                    conn=conn  # Pass the existing connection
                                )
                                if beer_id:
                                    # Get content for the row for change tracking
                                    content = self.change_tracker._get_row_content("beers", beer_id, conn)
                                    content_hash = hashlib.md5(content.encode()).hexdigest()
                                    
                                    # Store change to log later
                                    batch_changes.append({
                                        "table_name": "beers",
                                        "operation": "INSERT",
                                        "row_id": beer_id,
                                        "content": content,
                                        "content_hash": content_hash
                                    })
                                    batch_success_count += 1
                                    logger.info(f"Added beer '{beer_data.get('Name')}' with ID {beer_id}")
                            
                        except Exception as e:
                            logger.error(f"Error processing beer {batch_start + idx + 1}: {str(e)}")
                            errors.append(f"Error on beer {batch_start + idx + 1}: {str(e)}")
                    
                    # Continue only if we have successful operations in this batch
                    if batch_success_count > 0 and batch_changes:
                        # Now increment the Lamport clock with retry logic
                        max_retries = 5
                        retry_delay = 0.5
                        retries = 0
                        new_clock = None
                        
                        while retries < max_retries and new_clock is None:
                            try:
                                # Increment with longer timeout
                                new_clock = self.change_tracker.increment_logical_clock(
                                    max_retries=3,
                                    retry_delay=1.0
                                )
                                
                                if new_clock is None or new_clock == 0:
                                    # Fallback if increment failed
                                    logger.warning("Failed to increment logical clock, attempting to force update")
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT logical_clock FROM version WHERE id = 1")
                                    row = cursor.fetchone()
                                    current_clock = row[0] if row and row[0] is not None else 0
                                    new_clock = current_clock + 1
                                    
                                    # Directly update the version table
                                    cursor.execute(
                                        "UPDATE version SET timestamp = ?, logical_clock = ? WHERE id = 1",
                                        (current_time, new_clock)
                                    )
                                    conn.commit()
                                    logger.info(f"Force updated logical clock to {new_clock}")
                            except sqlite3.OperationalError as e:
                                if "database is locked" in str(e):
                                    retries += 1
                                    logger.warning(f"Database locked when updating logical clock in import (attempt {retries}/{max_retries}), retrying in {retry_delay}s")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error(f"SQLite error updating logical clock in import: {e}")
                                    # Set a default value so we can continue
                                    new_clock = 1
                                    break
                            except Exception as e:
                                logger.error(f"Error updating logical clock in import: {e}")
                                # Set a default value so we can continue
                                new_clock = 1
                                break
                        
                        # If all retries failed, use a default value
                        if new_clock is None:
                            logger.error("Failed to update logical clock after multiple retries, using default value")
                            new_clock = 1
                        
                        cursor = conn.cursor()
                        
                        # Verify and update version table if needed
                        try:
                            cursor.execute("SELECT logical_clock FROM version WHERE id = 1")
                            row = cursor.fetchone()
                            if row is None or row[0] != new_clock:
                                cursor.execute(
                                    "UPDATE version SET timestamp = ?, logical_clock = ? WHERE id = 1",
                                    (current_time, new_clock)
                                )
                                if cursor.rowcount == 0:
                                    cursor.execute(
                                        "INSERT OR REPLACE INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (1, ?, ?, ?, ?)",
                                        (current_time, "0", new_clock, self.change_tracker.node_id)
                                    )
                        except Exception as e:
                            logger.error(f"Error verifying logical clock in version table: {e}")
                        
                        # Insert all change records with the same logical clock value
                        for change in batch_changes:
                            cursor.execute(
                                """
                                INSERT INTO change_log 
                                (table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    change["table_name"],
                                    change["operation"],
                                    change["row_id"],
                                    current_time,
                                    change["content"],
                                    change["content_hash"],
                                    new_clock,  # Use the same clock value for all changes in the batch
                                    self.change_tracker.node_id
                                )
                            )
                    
                    # Commit all changes in this batch
                    conn.commit()
                    
                    # Update totals
                    success_count += batch_success_count
                    changes_to_log.extend(batch_changes)
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Transaction error in batch {batch_start//BATCH_SIZE + 1}: {str(e)}")
                    errors.append(f"Transaction error in batch {batch_start//BATCH_SIZE + 1}: {str(e)}")
            
            # Small delay between batches to allow other operations
            time.sleep(0.1)
        
        # Send notification after all batches are processed
        if success_count > 0:
            try:
                # Verify the logical clock is properly set before notification
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT logical_clock FROM version WHERE id = 1")
                    row = cursor.fetchone()
                    clock_value = row[0] if row else None
                    
                    if clock_value is None or clock_value == 0:
                        logger.warning("Logical clock is still 0 before notify, attempting to fix")
                        # Force increment the logical clock
                        new_clock = 1
                        cursor.execute(
                            "UPDATE version SET logical_clock = ? WHERE id = 1",
                            (new_clock,)
                        )
                        conn.commit()
                
                self.notify_update()
                logger.info(f"Successfully imported {success_count} beers")
            except Exception as e:
                logger.error(f"Error notifying after import: {str(e)}")
                errors.append(f"Error notifying after import: {str(e)}")
            
        return (success_count, errors)
    
    def clear_all_beers(self):
        """
        Remove all beers from the database
        
        Returns:
            Number of beers cleared
        """
        # Get all beers first
        beers = self.get_all_beers()
        beer_count = len(beers)
        
        # Clear all taps first
        taps = self.get_all_taps()
        for tap in taps:
            if tap['idBeer']:
                self.update_tap(tap['idTap'], None, notify=False)
        
        # Delete all beers without individual notifications
        for beer in beers:
            self.delete_beer(beer['idBeer'], notify=False)
        
        # Send a single notification for all changes
        if beer_count > 0:
            self.notify_update()
            
        return beer_count
    
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
            
        # Get current taps
        existing_taps = self.get_all_taps()
        current_count = len(existing_taps)
        
        # If decreasing, delete excess taps
        if count < current_count:
            # Delete taps from highest number to lowest
            for i in range(current_count, count, -1):
                tap_id = i
                self.delete_tap(tap_id, notify=False)
        
        # If increasing, add new taps
        elif count > current_count:
            # Add new taps with sequential IDs
            for i in range(current_count + 1, count + 1):
                tap_id = i
                self.add_tap(tap_id, None, notify=False)
        
        # Send a single notification after all changes
        self.notify_update()
        return True 