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
                if peer != self:
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