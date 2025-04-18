"""
Database module for KegDisplay providing synced database functionality.
"""

from .database import DatabaseManager
from .change_tracker import ChangeTracker
from .sync.synchronizer import DatabaseSynchronizer
from .sync.network import NetworkManager

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
                description=None, brewed=None, kegged=None, tapped=None, notes=None):
        """Add a new beer to the database"""
        beer_id = self.db_manager.add_beer(name, abv, ibu, color, og, fg, 
                                         description, brewed, kegged, tapped, notes)
        self.change_tracker.log_change("beers", "INSERT", beer_id)
        self.notify_update()
        return beer_id
    
    def update_beer(self, beer_id, name=None, abv=None, ibu=None, color=None, og=None, fg=None,
                   description=None, brewed=None, kegged=None, tapped=None, notes=None):
        """Update an existing beer in the database"""
        success = self.db_manager.update_beer(beer_id, name, abv, ibu, color, 
                                            og, fg, description, brewed, 
                                            kegged, tapped, notes)
        if success:
            self.change_tracker.log_change("beers", "UPDATE", beer_id)
            self.notify_update()
        return success
    
    def delete_beer(self, beer_id):
        """Delete a beer from the database"""
        # First check for taps using this beer
        taps_with_beer = self.get_tap_with_beer(beer_id)
        
        # Update those taps first
        for tap_id in taps_with_beer:
            self.update_tap(tap_id, None)
        
        # Now delete the beer
        success = self.db_manager.delete_beer(beer_id)
        if success:
            self.change_tracker.log_change("beers", "DELETE", beer_id)
            self.notify_update()
        return success
    
    def get_beer(self, beer_id):
        """Get a beer by ID"""
        return self.db_manager.get_beer(beer_id)
    
    def get_all_beers(self):
        """Get all beers from the database"""
        return self.db_manager.get_all_beers()
    
    # ---- Tap Management Methods ----
    
    def add_tap(self, tap_id=None, beer_id=None):
        """Add a new tap to the database"""
        tap_id = self.db_manager.add_tap(tap_id, beer_id)
        if tap_id:
            self.change_tracker.log_change("taps", "INSERT", tap_id)
            self.notify_update()
        return tap_id
    
    def update_tap(self, tap_id, beer_id):
        """Update a tap's beer assignment"""
        success = self.db_manager.update_tap(tap_id, beer_id)
        if success:
            self.change_tracker.log_change("taps", "UPDATE", tap_id)
            self.notify_update()
        return success
    
    def delete_tap(self, tap_id):
        """Delete a tap from the database"""
        success = self.db_manager.delete_tap(tap_id)
        if success:
            self.change_tracker.log_change("taps", "DELETE", tap_id)
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
        
        for idx, beer_data in enumerate(beer_data_list):
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
                        existing_beer = self.get_beer(beer_id)
                    except (ValueError, TypeError):
                        beer_id = None
                
                # Disable automatic notifications for each beer change
                if existing_beer:
                    # Update existing beer
                    self._update_beer_without_notify(
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
                        notes=beer_data.get('Notes')
                    )
                else:
                    # Add new beer
                    self._add_beer_without_notify(
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
                
                success_count += 1
            except Exception as e:
                errors.append(f"Error on beer {idx+1}: {str(e)}")
        
        # Send a single notification after all operations are complete
        if success_count > 0:
            self.notify_update()
            
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
                self._update_tap_without_notify(tap['idTap'], None)
        
        # Delete all beers without individual notifications
        for beer in beers:
            self._delete_beer_without_notify(beer['idBeer'])
        
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
                self._delete_tap_without_notify(tap_id)
        
        # If increasing, add new taps
        elif count > current_count:
            # Add new taps with sequential IDs
            for i in range(current_count + 1, count + 1):
                tap_id = i
                self._add_tap_without_notify(tap_id, None)
        
        # Send a single notification after all changes
        self.notify_update()
        return True
    
    # ---- Internal methods (no notification) ----
    
    def _add_beer_without_notify(self, name, abv=None, ibu=None, color=None, og=None, fg=None, 
                   description=None, brewed=None, kegged=None, tapped=None, notes=None):
        """Add a beer without sending notification"""
        beer_id = self.db_manager.add_beer(name, abv, ibu, color, og, fg, 
                                         description, brewed, kegged, tapped, notes)
        self.change_tracker.log_change("beers", "INSERT", beer_id)
        return beer_id
    
    def _update_beer_without_notify(self, beer_id, name=None, abv=None, ibu=None, color=None, og=None, fg=None,
                   description=None, brewed=None, kegged=None, tapped=None, notes=None):
        """Update a beer without sending notification"""
        success = self.db_manager.update_beer(beer_id, name, abv, ibu, color, 
                                            og, fg, description, brewed, 
                                            kegged, tapped, notes)
        if success:
            self.change_tracker.log_change("beers", "UPDATE", beer_id)
        return success
    
    def _delete_beer_without_notify(self, beer_id):
        """Delete a beer without sending notification"""
        success = self.db_manager.delete_beer(beer_id)
        if success:
            self.change_tracker.log_change("beers", "DELETE", beer_id)
        return success
    
    def _add_tap_without_notify(self, tap_id=None, beer_id=None):
        """Add a tap without sending notification"""
        tap_id = self.db_manager.add_tap(tap_id, beer_id)
        if tap_id:
            self.change_tracker.log_change("taps", "INSERT", tap_id)
        return tap_id
    
    def _update_tap_without_notify(self, tap_id, beer_id):
        """Update a tap without sending notification"""
        success = self.db_manager.update_tap(tap_id, beer_id)
        if success:
            self.change_tracker.log_change("taps", "UPDATE", tap_id)
        return success
    
    def _delete_tap_without_notify(self, tap_id):
        """Delete a tap without sending notification"""
        success = self.db_manager.delete_tap(tap_id)
        if success:
            self.change_tracker.log_change("taps", "DELETE", tap_id)
        return success 