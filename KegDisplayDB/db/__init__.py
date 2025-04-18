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
        if self.test_mode:
            # In test mode, directly sync with test peers
            for peer in self.test_peers:
                if peer != self and hasattr(peer, 'synchronizer') and peer.synchronizer:
                    # Only sync with peer if both have synchronizers
                    if hasattr(self, 'synchronizer') and self.synchronizer:
                        self.synchronizer._sync_with_peer(peer)
        else:
            # Use synchronizer to broadcast update
            self.synchronizer.notify_update()
    
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
            beer_data_list: List of dictionaries with beer data
            
        Returns:
            Tuple of (success_count, errors)
        """
        success_count = 0
        errors = []
        changes_to_log = []
        current_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Process beers in smaller batches to prevent long transactions
        BATCH_SIZE = 10
        total_beers = len(beer_data_list)
        
        for batch_start in range(0, total_beers, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_beers)
            batch = beer_data_list[batch_start:batch_end]
            
            # Single transaction for the current batch
            with self.db_manager.get_connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("PRAGMA busy_timeout = 10000")  # 10 second timeout
                
                try:
                    # Process current batch of beers
                    batch_changes = []
                    batch_success_count = 0
                    
                    for idx, beer_data in enumerate(batch):
                        try:
                            # Skip rows without a name
                            if not beer_data.get('Name'):
                                continue
                            
                            # Handle existing beer with same ID
                            beer_id = beer_data.get('idBeer')
                            existing_beer = None
                            
                            if beer_id and str(beer_id).strip() and str(beer_id) != '':
                                try:
                                    beer_id = int(beer_id)
                                    existing_beer = self.db_manager.get_beer(beer_id)
                                except (ValueError, TypeError):
                                    beer_id = None
                            
                            # Use database operations
                            if existing_beer:
                                # Update existing beer
                                success = self.db_manager.update_beer(
                                    beer_id=beer_id,
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
                                if success:
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
                        # Now increment the Lamport clock
                        new_clock = self.change_tracker.increment_logical_clock()
                        
                        cursor = conn.cursor()
                        
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
                        
                        # Update version table with the new clock value
                        cursor.execute(
                            "UPDATE version SET timestamp = ?, logical_clock = ? WHERE id = 1",
                            (current_time, new_clock)
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