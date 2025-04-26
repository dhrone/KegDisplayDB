"""
Change tracking module for KegDisplay.
Handles tracking and recording database changes.
"""

import sqlite3
import logging
import hashlib
import os
from datetime import datetime, UTC
import json
import uuid
import time

logger = logging.getLogger("KegDisplay")

class ChangeTracker:
    """
    Handles change tracking for the KegDisplay system.
    Manages the change_log and version tables, tracks changes,
    and provides utilities for managing database versions.
    """
    
    def __init__(self, db_manager):
        """Initialize the change tracker
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
        self.initialize_tracking()
        self.node_id = self.initialize_node_id()
        # For cache invalidation notifications
        self.version_cache_callbacks = []
        logger.info(f"ChangeTracker initialized with node ID: {self.node_id}")
    
    def initialize_tracking(self):
        """Initialize the change tracking tables if empty
        
        Uses DBService to ensure thread-safety and consistency
        """
        try:
            def init_transaction(conn):
                # Check if version table is empty
                cursor = conn.execute("SELECT COUNT(*) FROM version")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    node_id = str(uuid.uuid4())
                    conn.execute(
                        "INSERT INTO version (timestamp, hash, logical_clock, node_id) VALUES (?, ?, ?, ?)",
                        (now, "0", 0, node_id)
                    )
                    logger.info("Initialized version table with new record")
                return True
                
            # Execute through DBService
            self.db_manager.transaction(init_transaction)
            logger.info("Change tracking tables initialized with Lamport clock support")
        except Exception as e:
            logger.error(f"Error initializing change tracking: {e}")
    
    def initialize_node_id(self):
        """Create a persistent unique node ID for this instance
        
        Uses DBService execute for reads and only creates a transaction when updates are needed
        
        Returns:
            str: The node ID
        """
        try:
            # First check if node_id already exists using a simple execute call
            result = self.db_manager.execute("SELECT node_id FROM version WHERE id = 1")
            
            # If we found a valid node_id, return it immediately
            if result and result[0][0]:
                node_id = result[0][0]
                logger.info(f"Using existing node ID: {node_id}")
                return node_id
            
            # Check if the version row exists at all
            row_exists = False
            result = self.db_manager.dbs.execute("SELECT COUNT(*) FROM version WHERE id = 1")
            if result and result[0][0] > 0:
                row_exists = True
            
            # Generate a new node ID
            node_id = str(uuid.uuid4())
            
            # Use the appropriate SQL based on whether the row exists
            if row_exists:
                # Simple update if the row exists
                self.db_manager.execute(
                    "UPDATE version SET node_id = ? WHERE id = 1",
                    (node_id,)
                )
            else:
                # Insert a new row if it doesn't exist
                timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                self.db_manager.execute(
                    "INSERT INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (?, ?, ?, ?, ?)",
                    (1, timestamp, "0", 0, node_id)
                )
            
            logger.info(f"Initialized new node ID: {node_id}")
            return node_id
            
        except Exception as e:
            logger.error(f"Error initializing node ID: {e}")
            # Fallback to a temporary ID
            temp_id = f"temp-{int(time.time())}"
            logger.warning(f"Using temporary node ID: {temp_id}")
            return temp_id
    

    def get_current_clock(self):
        """Get the current logical clock value
        
        Returns:
            int: Current logical clock value
        """
        # Get current logical clock value or zero if no row exists
        current_clock_row = self.db_manager.execute("SELECT logical_clock FROM version WHERE id = 1")
        current_clock_row = current_clock_row[0] if current_clock_row else (-1,)
        return current_clock_row[0]
    

    def register_cache_callback(self, callback):
        """Register a callback function to be called when version changes
        
        Args:
            callback: Function to call when version changes
        """
        if callback not in self.version_cache_callbacks:
            self.version_cache_callbacks.append(callback)
            
    def notify_version_change(self):
        """Notify all registered callbacks about a version change"""
        for callback in self.version_cache_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Error in version cache callback: {e}")
    
    def increment_logical_clock(self, received_clock=None):
        """Increment the logical clock
        
        Args:
            received_clock: Optional external clock value to incorporate (for Lamport clock protocol)

            
        Returns:
            int: New logical clock value
        """
        try:
            current_clock = self.get_current_clock()

            # Lamport clock rule: local_clock = max(local_clock, received_clock) + 1
            new_clock = max(current_clock, received_clock if received_clock else 0) + 1            

            # Calculate a fresh content hash
            tables = ['beers', 'taps']
            content_hash = self.db_manager._calculate_db_hash(tables)
            timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            if current_clock <0:
                # Insert new version record if none exists
                self.db_manager.execute(
                    "INSERT INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (?, ?, ?, ?, ?)",
                    (1, timestamp, content_hash, new_clock, self.node_id)
                )
                log_message = f"Incremented a new logical clock to {new_clock}"
            else:
                # Update existing version record
                self.db_manager.execute(
                    """
                    UPDATE version
                        SET timestamp     = ?,
                        hash          = ?,
                        logical_clock = ?,
                        node_id       = ?
                    WHERE id = 1
                    """,
                    (timestamp, content_hash, new_clock, self.node_id)
                )
                log_message = f"Incremented logical clock from {current_clock} to {new_clock}"
            self.logical_clock = new_clock
            logger.debug(log_message)
            
            # Notify any registered cache callbacks
            self.notify_version_change()
            
            return new_clock

                
        except Exception as e:
            logger.error(f"Error incrementing logical clock: {e}")
            return 0  # Return 0 as a safe default
    
    def update_logical_clock(self, received_clock):
        """
        Update logical clock based on received clock value (Lamport algorithm)
        and persist it to the version table.
        
        Args:
            received_clock: Clock value received from another node
            
        Returns:
            New clock value or None if unsuccessful
        """
        return self.increment_logical_clock(received_clock)
    
    def clear_logical_clock(self):
        """Clear the logical clock
        
        Returns:
            success: True if the operation was successful
        """
        return self.db_manager.execute("UPDATE version SET logical_clock = 0 WHERE id = 1")
    
    def ensure_valid_session(self):
        """Ensure we have a valid tracking session"""
        try:
            # Check if the change_log table exists
            change_log_exists = self.db_manager.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='change_log'"
            )
            
            if not change_log_exists:
                # Table doesn't exist, needs initialization
                raise sqlite3.Error("change_log table missing")
                
            # Check if the version table exists
            version_exists = self.db_manager.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='version'"
            )
            
            if not version_exists:
                # Table doesn't exist, needs initialization
                raise sqlite3.Error("version table missing")
                
        except sqlite3.Error as e:
            # If session is invalid (tables missing or other DB error during check)
            logger.warning(f"Session invalid ({e}), attempting reinitialization")
            self.initialize_tracking()
            self.node_id = self.initialize_node_id()
    
    def log_change(self, table_name, operation, row_id,
                   increment_clock=True, record_node_id=None):
        """
        Atomically log a database change and bump the Lamport clock,
        then notify any version‐cache listeners.
        """
        # 1. Compute new logical clock in‐memory
        current_clock = self.get_current_clock()
        new_clock = (max(current_clock, 0) + 1) if increment_clock else current_clock

        # 2. Capture row content & hashes up front
        content = self._get_row_content(table_name, row_id)
        content_hash = hashlib.md5(content.encode()).hexdigest()
        timestamp = datetime.now(UTC).isoformat()
        if record_node_id is None:
            record_node_id = self.node_id

        # 3. Compute overall DB content hash
        db_content_hash = self._calculate_content_hash()

        # 4. Wrap both writes in one transaction
        def _txn(conn):
            # insert into change_log
            conn.execute(
                """
                INSERT INTO change_log
                  (table_name, operation, row_id, timestamp,
                   content, content_hash, logical_clock, node_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (table_name, operation, row_id,
                 timestamp, content, content_hash,
                 new_clock, record_node_id)
            )
            # update (or insert) version row
            cur = conn.execute(
                """
                UPDATE version
                   SET timestamp     = ?,
                       hash          = ?,
                       logical_clock = ?,
                       node_id       = ?
                 WHERE id = 1
                """,
                (timestamp, db_content_hash, new_clock, self.node_id)
            )
            if cur.rowcount == 0:
                conn.execute(
                    "INSERT INTO version (timestamp, hash, logical_clock, node_id) VALUES (?, ?, ?, ?)",
                    (timestamp, db_content_hash, new_clock, self.node_id)
                )
            return new_clock

        # 5. Execute transaction and get new clock
        new_clock = self.db_manager.transaction(_txn)

        # 6. Notify callbacks that version changed
        self.notify_version_change()

        return new_clock    

    def get_changes_since_clock(self, last_clock, peer_node_id=None, batch_size=1000):
        """Get all changes since a given logical clock value
        
        Args:
            last_clock: Logical clock value to get changes since
            peer_node_id: Node ID for tie-breaking (optional)
            batch_size: Maximum number of changes to return
            
        Returns:
            changes: List of changes
        """

        try:
            # Get all changes with higher logical clock, excluding those from the peer node
            # (they already have their own changes)
            query_params = [last_clock]
            higher_clock_query = '''
                SELECT table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id
                FROM change_log
                WHERE logical_clock > ?
            '''
            
            if peer_node_id:
                higher_clock_query += ' AND node_id != ?'
                query_params.append(peer_node_id)
                
            higher_clock_query += ' ORDER BY logical_clock, node_id'
            
            higher_clock_changes = self.db_manager.execute(
                higher_clock_query,
                tuple(query_params)
            ) or []
            
            # Get changes with equal clock but from different nodes
            # (only if peer_node_id is provided)
            equal_clock_changes = []
            if peer_node_id:
                equal_clock_changes = self.db_manager.execute(
                    '''
                    SELECT table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id
                    FROM change_log
                    WHERE logical_clock = ? AND node_id != ?
                    ORDER BY logical_clock, node_id
                    ''',
                    (last_clock, peer_node_id)
                ) or []
                
            # Combine and sort the changes
            all_changes = higher_clock_changes + equal_clock_changes
            all_changes.sort(key=lambda x: (x[6], x[7]))  # Sort by logical_clock, then node_id
            
            # Log details about the number of changes found
            if all_changes:
                logger.debug(f"Found {len(higher_clock_changes)} changes with higher clock value than {last_clock}")
                if equal_clock_changes:
                    logger.debug(f"Found {len(equal_clock_changes)} changes with equal clock value from different nodes")
            
            # Limit to batch_size
            if len(all_changes) > batch_size:
                logger.warning(f"Limiting changes from {len(all_changes)} to {batch_size}")
                all_changes = all_changes[:batch_size]
            
            if len(all_changes) > 0:
                logger.info(f"Found {len(all_changes)} changes since logical clock {last_clock}")
            else:
                logger.debug(f"No changes found since logical clock {last_clock}")

            return all_changes
            
        except Exception as e:
            logger.error(f"Error getting changes since clock {last_clock}: {e}")
            return []
 
    
    def get_db_version(self):
        """Calculate database version based on content and Lamport clock
        
        Args:
            conn: Optional database connection to use within an existing transaction
            
        Returns:
            dict: Database version information including hash, timestamp, logical clock, and node ID
        """

        try:
            # Get timestamp, hash, logical_clock, and node_id from version table
            version_rows = self.db_manager.execute(
                "SELECT timestamp, hash, logical_clock, node_id FROM version WHERE id = 1 LIMIT 1"
            )
            
            if version_rows and len(version_rows) > 0:
                # Get the first row from results
                version_row = version_rows[0]
                
                # Make sure the row has enough elements
                if len(version_row) >= 4:
                    timestamp, stored_hash, logical_clock, node_id = version_row
                    logical_clock = logical_clock if logical_clock is not None else 0
                    node_id = node_id if node_id else self.node_id
                
                    # Return version information
                    return {
                        'hash': stored_hash,
                        'timestamp': timestamp,
                        'logical_clock': logical_clock,
                        'node_id': node_id
                    }
            
            # If we get here, something went wrong with retrieving the version info
            logger.warning("Could not retrieve version information from database, returning default version")
            
        except Exception as e:
            logger.error(f"Error getting version information from database: {e}")
            
        # Return a default version if we couldn't get from DB
        return {
            'hash': '0',
            'timestamp': datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            'logical_clock': 0,
            'node_id': self.node_id
        }

    
    def _get_row_content(self, table_name, row_id):
        """Get the content of a row as a JSON string for change tracking
        
        Args:
            table_name: Table name
            row_id: Row ID
            conn: Optional database connection to use within an existing transaction
            
        Returns:
            content: JSON string representation of the row
        """

        try:
            # Get row data
            rows = self.db_manager.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (row_id,))
            
            # Check if we got any results
            if rows and len(rows) > 0:
                row = rows[0]  # Get the first row
                
                # Get column names
                columns_info = self.db_manager.execute(f"PRAGMA table_info({table_name})")
                
                # Make sure we have column info
                if columns_info and len(columns_info) > 0:
                    column_names = [info[1] for info in columns_info]
                    
                    # Make sure we have the right number of columns
                    if len(column_names) == len(row):
                        # Convert row to dict for JSON serialization
                        row_dict = {}
                        for i, col in enumerate(column_names):
                            row_dict[col] = row[i]
                        
                        return json.dumps(row_dict)
            
            # If we get here, something went wrong with getting the data
            logger.warning(f"Could not fully retrieve data for {table_name}.{row_id}, returning empty object")
            return "{}"
        except Exception as e:
            logger.error(f"Error getting row content for {table_name}.{row_id}: {e}")
            return "{}"

    
    def is_database_empty(self):
        """Check if the database is empty (no beers or taps)
        
        Args:
            conn: Optional database connection to use (to avoid nested transactions)
        
        Returns:
            bool: True if database has no content, False otherwise
        """
        try:
            # Check beers table
            beer_count = self.db_manager.execute("SELECT COUNT(*) FROM beers",)
            
            # Check taps table
            tap_count = self.db_manager.execute("SELECT COUNT(*) FROM taps")
            
            return beer_count[0][0] == 0 and tap_count[0][0] == 0
        except Exception as e:
            logger.error(f"Error checking if database is empty: {e}")
            return True  # Assume empty if we can't check

    def is_newer_version(self, version1, version2):
        """Determine if version1 is newer than version2 based on logical clocks
        
        Args:
            version1: Version dict with logical_clock and node_id
            version2: Version dict with logical_clock and node_id
            
        Returns:
            is_newer: True if version1 is newer than version2
        """
        # If hashes are the same, they are the same version
        if version1.get("hash") == version2.get("hash"):
            return False
            
        # Compare logical clocks (primary comparison)
        clock1 = version1.get("logical_clock", 0)
        clock2 = version2.get("logical_clock", 0)
        
        if clock1 > clock2:
            logger.debug(f"Version1 has higher logical clock: {clock1} > {clock2}")
            return True
        elif clock1 < clock2:
            logger.debug(f"Version1 has lower logical clock: {clock1} < {clock2}")
            return False
        
        # If logical clocks are equal, use node_id for tie-breaking
        node_id1 = version1.get("node_id", "")
        node_id2 = version2.get("node_id", "")
        
        # If node IDs are the same, they are the same version
        if node_id1 == node_id2:
            return False
        
        # Arbitrary but consistent tie-breaking: lexicographically higher node ID wins
        is_newer = node_id1 > node_id2
        winner = node_id1 if is_newer else node_id2
        logger.debug(f"Tie-breaking with node IDs: {node_id1} vs {node_id2}, result: {winner}")
        return is_newer 

    def _calculate_content_hash(self):
        """Calculate content hash for database
        
        This delegates to the DatabaseManager._calculate_db_hash method which
        computes a hash based on the content of tracked tables.
        
        Returns:
            str: Hash of database content
        """
        try:
            # Use the db_manager's _calculate_db_hash method to get content hash
            tracked_tables = ['beers', 'taps']
            return self.db_manager._calculate_db_hash(tracked_tables)
        except Exception as e:
            logger.error(f"Error calculating content hash: {e}")
            return "0"  # Default hash
