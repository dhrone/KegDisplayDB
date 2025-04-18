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
        logger.info(f"ChangeTracker initialized with node ID: {self.node_id}")
    
    def initialize_tracking(self):
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Initialize version table if empty
                cursor.execute("SELECT COUNT(*) FROM version")
                if cursor.fetchone()[0] == 0:
                    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    node_id = str(uuid.uuid4())
                    cursor.execute(
                        "INSERT INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (1, ?, ?, 0, ?)",
                        (now, "0", node_id)
                    )
                
                conn.commit()
                logger.info("Change tracking tables initialized with Lamport clock support")
        except Exception as e:
            logger.error(f"Error initializing change tracking: {e}")
    
    def initialize_node_id(self):
        """Create a persistent unique node ID for this instance
        
        Returns:
            str: The node ID
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if node_id exists in version table
                cursor.execute("SELECT node_id FROM version WHERE id = 1")
                row = cursor.fetchone()
                
                if row and row[0]:
                    logger.info(f"Using existing node ID: {row[0]}")
                    return row[0]
                
                # Generate a new node ID if none exists
                node_id = str(uuid.uuid4())
                
                # Store it permanently
                cursor.execute("UPDATE version SET node_id = ? WHERE id = 1", (node_id,))
                conn.commit()
                
                logger.info(f"Initialized new node ID: {node_id}")
                return node_id
        except Exception as e:
            logger.error(f"Error initializing node ID: {e}")
            # Fallback to a temporary ID
            temp_id = f"temp-{int(time.time())}"
            logger.warning(f"Using temporary node ID: {temp_id}")
            return temp_id
    
    def increment_logical_clock(self):
        """Increment the logical clock for local events
        
        Returns:
            int: The new clock value
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT logical_clock FROM version WHERE id = 1")
                row = cursor.fetchone()
                current_clock = row[0] if row and row[0] is not None else 0
                new_clock = current_clock + 1
                
                cursor.execute(
                    "UPDATE version SET logical_clock = ? WHERE id = 1",
                    (new_clock,)
                )
                conn.commit()
                logger.debug(f"Incremented logical clock to {new_clock}")
                return new_clock
        except Exception as e:
            logger.error(f"Error incrementing logical clock: {e}")
            return 0
    
    def update_logical_clock(self, received_clock):
        """Update logical clock based on received clock value (Lamport algorithm)
        
        Args:
            received_clock: Clock value received from another node
            
        Returns:
            int: The new clock value
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT logical_clock FROM version WHERE id = 1")
                row = cursor.fetchone()
                current_clock = row[0] if row and row[0] is not None else 0
                
                # Lamport clock rule: local_clock = max(local_clock, received_clock) + 1
                new_clock = max(current_clock, received_clock) + 1
                
                cursor.execute(
                    "UPDATE version SET logical_clock = ? WHERE id = 1",
                    (new_clock,)
                )
                conn.commit()
                logger.debug(f"Updated logical clock to {new_clock} based on received clock {received_clock}")
                return new_clock
        except Exception as e:
            logger.error(f"Error updating logical clock: {e}")
            return 0
    
    def set_logical_clock(self, new_clock):
        """Set the logical clock to a specific value
        
        Args:
            new_clock: The clock value to set
            
        Returns:
            int: The new clock value
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE version SET logical_clock = ? WHERE id = 1",
                    (new_clock,)
                )
                conn.commit()
                logger.debug(f"Set logical clock to exact value: {new_clock}")
                return new_clock
        except Exception as e:
            logger.error(f"Error setting logical clock: {e}")
            return 0
    
    def ensure_valid_session(self):
        """Ensure we have a valid tracking session"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                # Directly check if the change_log table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='change_log'")
                if cursor.fetchone() is None:
                    # Table doesn't exist, needs initialization
                    raise sqlite3.Error("change_log table missing") 

                # Check for the version table too
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='version'")
                if cursor.fetchone() is None:
                    raise sqlite3.Error("version table missing")

        except sqlite3.Error as e:
            # If session is invalid (tables missing or other DB error during check)
            logger.warning(f"Session invalid ({e}), attempting reinitialization")
            self.initialize_tracking()
            self.node_id = self.initialize_node_id()
    
    def log_change(self, table_name, operation, row_id):
        """Log a database change with Lamport logical clock
        
        Args:
            table_name: Name of the table that changed
            operation: Operation type (INSERT, UPDATE, DELETE)
            row_id: ID of the row that changed
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Increment logical clock for this operation
                new_clock = self.increment_logical_clock()
                
                # Get content for the row
                content = self._get_row_content(table_name, row_id, conn)
                content_hash = hashlib.md5(content.encode()).hexdigest()
                
                # Set current timestamp (still kept for reference)
                timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Log the change with logical clock and node ID
                cursor.execute(
                    """
                    INSERT INTO change_log 
                    (table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (table_name, operation, row_id, timestamp, content, content_hash, new_clock, self.node_id)
                )
                
                # Update version table with new logical clock
                cursor.execute(
                    "UPDATE version SET timestamp = ?, logical_clock = ? WHERE id = 1",
                    (timestamp, new_clock)
                )
                if cursor.rowcount == 0:
                    # If no rows were updated, insert a new row
                    cursor.execute(
                        "INSERT INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (1, ?, ?, ?, ?)",
                        (timestamp, "0", new_clock, self.node_id)
                    )
            
                conn.commit()
                logger.debug(f"Logged {operation} operation on {table_name} for row {row_id} with logical clock {new_clock}")
        except sqlite3.Error as e:
            logger.error(f"Error logging change: {e}")
        except Exception as e:
            logger.error(f"Unexpected error logging change: {e}")
    
    def get_changes_since(self, last_timestamp, batch_size=1000):
        """Get all changes since a given timestamp
        
        Args:
            last_timestamp: Timestamp to get changes since
            batch_size: Maximum number of changes to return
            
        Returns:
            changes: List of changes
        """
        # Legacy function, maintained for backward compatibility
        # In the future, this could be replaced with a get_changes_since_clock method
        
        # Make sure we have a valid session
        self.ensure_valid_session()
        
        # Try to normalize the timestamp format for consistent comparison
        try:
            # Determine the timestamp format and parse accordingly
            if '.' in last_timestamp:
                # If it has milliseconds
                dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                dt = dt.replace(tzinfo=UTC)
            elif 'Z' in last_timestamp:
                # If it has Z timezone indicator
                dt = datetime.strptime(last_timestamp.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=UTC)
            else:
                # Basic ISO format without timezone
                dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=UTC)
        except ValueError as e:
            logger.warning(f"Error parsing timestamp '{last_timestamp}': {e}")
            dt = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)  # Use epoch start if parsing fails
            
        # Convert back to a standard ISO format without timezone for consistent comparison
        normalized_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.debug(f"Normalized timestamp from '{last_timestamp}' to '{normalized_timestamp}'")
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # First, get all changes from the change_log table
            cursor.execute('''
                SELECT table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id
                FROM change_log
                ORDER BY logical_clock
            ''')
            
            all_changes = cursor.fetchall()
            
            # Filter changes that have a timestamp after our normalized_timestamp
            # by using timestamp comparison on parsed datetime objects
            filtered_changes = []
            for change in all_changes:
                change_timestamp = change[3]  # timestamp is at index 3
                
                try:
                    # Parse the change timestamp
                    if '.' in change_timestamp:
                        change_dt = datetime.strptime(change_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                    elif 'Z' in change_timestamp:
                        change_dt = datetime.strptime(change_timestamp.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                    else:
                        change_dt = datetime.strptime(change_timestamp, "%Y-%m-%dT%H:%M:%S")
                    
                    # Convert to UTC for consistent comparison
                    change_dt = change_dt.replace(tzinfo=UTC)
                    
                    # Compare with our normalized timestamp
                    if change_dt > dt:
                        filtered_changes.append(change)
                except ValueError as e:
                    logger.warning(f"Error parsing change timestamp '{change_timestamp}': {e}")
                    # Skip this change if we can't parse its timestamp
            
            # Limit to batch_size
            if len(filtered_changes) > batch_size:
                logger.warning(f"Limiting changes from {len(filtered_changes)} to {batch_size}")
                filtered_changes = filtered_changes[:batch_size]
            
            logger.info(f"Found {len(filtered_changes)} changes since {normalized_timestamp}")
            return filtered_changes

    def get_changes_since_clock(self, last_clock, node_id=None, batch_size=1000):
        """Get all changes since a given logical clock value
        
        Args:
            last_clock: Logical clock value to get changes since
            node_id: Node ID for tie-breaking (optional)
            batch_size: Maximum number of changes to return
            
        Returns:
            changes: List of changes
        """
        # Make sure we have a valid session
        self.ensure_valid_session()
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all changes with higher logical clock
            cursor.execute('''
                SELECT table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id
                FROM change_log
                WHERE logical_clock > ?
                ORDER BY logical_clock
            ''', (last_clock,))
            
            higher_clock_changes = cursor.fetchall()
            
            # Get changes with equal clock but from different nodes
            # (only if node_id is provided)
            if node_id:
                cursor.execute('''
                    SELECT table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id
                    FROM change_log
                    WHERE logical_clock = ? AND node_id != ?
                    ORDER BY node_id
                ''', (last_clock, node_id))
                
                equal_clock_changes = cursor.fetchall()
                
                # Combine and sort the changes
                all_changes = higher_clock_changes + equal_clock_changes
                all_changes.sort(key=lambda x: (x[6], x[7]))  # Sort by logical_clock, then node_id
            else:
                all_changes = higher_clock_changes
            
            # Limit to batch_size
            if len(all_changes) > batch_size:
                logger.warning(f"Limiting changes from {len(all_changes)} to {batch_size}")
                all_changes = all_changes[:batch_size]
            
            logger.info(f"Found {len(all_changes)} changes since logical clock {last_clock}")
            return all_changes
    
    def get_db_version(self):
        """Calculate database version based on content and Lamport clock
        
        Returns:
            version: Dictionary with hash, logical_clock, and node_id
        """
        if not os.path.exists(self.db_manager.db_path):
            return {"hash": "0", "logical_clock": 0, "node_id": self.node_id}
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Calculate content-based hash from all tracked tables
                tables = ['beers', 'taps']
                content_hashes = []
                for table in tables:
                    content_hashes.append(self._get_table_hash(table))
                content_hash = hashlib.md5(''.join(content_hashes).encode()).hexdigest()
                
                # Get version information from version table
                try:
                    # Get timestamp, hash, logical_clock, and node_id from version table
                    cursor.execute("SELECT timestamp, hash, logical_clock, node_id FROM version WHERE id = 1 LIMIT 1")
                    row = cursor.fetchone()
                    
                    if row:
                        timestamp, stored_hash, logical_clock, node_id = row
                        logical_clock = logical_clock if logical_clock is not None else 0
                        node_id = node_id if node_id else self.node_id
                        
                        # Always update the hash to ensure it's correct
                        cursor.execute(
                            "UPDATE version SET hash = ?, node_id = ? WHERE id = 1", 
                            (content_hash, self.node_id)
                        )
                        conn.commit()
                    else:
                        timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                        logical_clock = 0
                        node_id = self.node_id
                        cursor.execute(
                            "INSERT INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (1, ?, ?, ?, ?)",
                            (timestamp, content_hash, logical_clock, node_id)
                        )
                        conn.commit()
                except sqlite3.Error as e:
                    logger.error(f"Error accessing version table: {e}")
                    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    logical_clock = 0
                    node_id = self.node_id
                    
                    # Try to create the version table with proper schema
                    try:
                        cursor.execute('''
                            CREATE TABLE IF NOT EXISTS version (
                                id INTEGER PRIMARY KEY,
                                timestamp TEXT NOT NULL,
                                hash TEXT NOT NULL,
                                logical_clock INTEGER DEFAULT 0,
                                node_id TEXT
                            )
                        ''')
                        cursor.execute(
                            "INSERT INTO version (id, timestamp, hash, logical_clock, node_id) VALUES (1, ?, ?, ?, ?)",
                            (timestamp, content_hash, logical_clock, node_id)
                        )
                        conn.commit()
                    except sqlite3.Error as e2:
                        logger.error(f"Error creating version table: {e2}")
        except Exception as e:
            logger.error(f"Error getting database version: {e}")
            return {"hash": "0", "logical_clock": 0, "node_id": self.node_id}
            
        # Building the version object with logical clock and node_id
        # We keep timestamp for backward compatibility
        return {
            "hash": content_hash, 
            "timestamp": timestamp, 
            "logical_clock": logical_clock,
            "node_id": node_id
        }
    
    def _get_row_content(self, table_name, row_id, conn=None):
        """Get the content of a row as a JSON string for change tracking
        
        Args:
            table_name: Table name
            row_id: Row ID
            conn: Database connection (optional)
            
        Returns:
            content: JSON string representation of the row
        """
        # Get or create a database connection
        close_conn = False
        if conn is None:
            conn = self.db_manager.get_connection()
            close_conn = True
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (row_id,))
            row = cursor.fetchone()
            
            if row:
                # Convert row to dict for JSON serialization
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [info[1] for info in cursor.fetchall()]
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                
                return json.dumps(row_dict)
            else:
                return "{}"
        except Exception as e:
            logger.error(f"Error getting row content for {table_name}.{row_id}: {e}")
            return "{}"
        finally:
            if close_conn:
                conn.close()
    
    def _get_table_hash(self, table_name):
        """Calculate a hash of the table's contents
        
        Args:
            table_name: Name of the table to hash
            
        Returns:
            hash: MD5 hash of the table's contents
        """
        try:
            # Check if a non-empty table exists
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(f"SELECT count(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    if count == 0:
                        return "0"
                    
                    # Get column names for sorting and consistent data representation
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    column_info = cursor.fetchall()
                    column_names = [col[1] for col in column_info]  # col[1] is the column name in PRAGMA result
                    
                    # Query all rows from the table
                    cursor.execute(f"SELECT * FROM {table_name}")
                    rows = cursor.fetchall()
                    
                    # Create a sorted, normalized representation for hashing
                    normalized_data = []
                    for row in rows:
                        # Sort the values by column name to ensure consistent ordering
                        row_dict = {}
                        for i, col_name in enumerate(column_names):
                            # Convert value to string, handle None values
                            val = row[i]
                            if val is None:
                                val = "NULL"
                            else:
                                val = str(val)
                            row_dict[col_name] = val
                        normalized_data.append(row_dict)
                    
                    # Sort the normalized data to ensure consistent ordering
                    normalized_data.sort(key=lambda x: [str(x.get(col, "")) for col in column_names])
                    
                    # Convert to JSON and calculate hash
                    data_json = json.dumps(normalized_data, sort_keys=True)
                    table_hash = hashlib.md5(data_json.encode()).hexdigest()
                    
                    return table_hash
                except Exception as e:
                    logger.error(f"Error calculating hash for table {table_name}: {e}")
                    return "0"
        except Exception as e:
            logger.error(f"Error accessing table {table_name}: {e}")
            return "0"
    
    def is_newer_version(self, version1, version2):
        """Determine if version1 is newer than version2 based on logical clocks
        
        Args:
            version1: Version dict with logical_clock and node_id
            version2: Version dict with logical_clock and node_id
            
        Returns:
            is_newer: True if version1 is newer than version2
        """
        # If version2 is empty database, any other version is newer
        if version2.get("hash") == "0":
            return True
        
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
        logger.debug(f"Tie-breaking with node IDs: {node_id1} vs {node_id2}, result: {is_newer}")
        return is_newer 