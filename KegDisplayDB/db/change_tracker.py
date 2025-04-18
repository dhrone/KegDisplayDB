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
    
    def initialize_tracking(self):
        """Initialize the change tracking system"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create change_log table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS change_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    row_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    content TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                )
            ''')
            
            # Create version table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS version (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    hash TEXT NOT NULL
                )
            ''')
            
            # Initialize version table if empty
            cursor.execute("SELECT COUNT(*) FROM version")
            if cursor.fetchone()[0] == 0:
                now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                cursor.execute(
                    "INSERT INTO version (id, timestamp, hash) VALUES (1, ?, ?)",
                    (now, "0")
                )
            
            conn.commit()
            logger.info("Change tracking tables initialized")
    
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
    
    def log_change(self, table_name, operation, row_id):
        """Log a database change
        
        Args:
            table_name: Name of the table being changed
            operation: Operation type (INSERT, UPDATE, DELETE)
            row_id: ID of the row being changed
        """
        self.ensure_valid_session()
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            # Use a consistent timestamp format: YYYY-MM-DDThh:mm:ssZ (without milliseconds)
            timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Get the actual row data for the change
            cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (row_id,))
            row = cursor.fetchone()
            if row:
                # Convert row data to a list and serialize to JSON
                content = json.dumps(list(row))
                # Calculate hash of the content for verification
                content_hash = hashlib.md5(content.encode()).hexdigest()
            else:
                content = "[]"
                content_hash = hashlib.md5(content.encode()).hexdigest()
            
            cursor.execute('''
                INSERT INTO change_log (table_name, operation, row_id, timestamp, content, content_hash)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (table_name, operation, row_id, timestamp, content, content_hash))
            
            # Update version timestamp 
            try:
                # Update timestamp field
                cursor.execute(
                    "UPDATE version SET timestamp = ? WHERE id = 1",
                    (timestamp,)
                )
                if cursor.rowcount == 0:
                    # If no rows were updated, insert a new row
                    cursor.execute(
                        "INSERT INTO version (id, timestamp, hash) VALUES (1, ?, ?)",
                        (timestamp, "0")
                    )
            except sqlite3.Error as e:
                logger.error(f"Error updating version table: {e}")
            
            conn.commit()
            
            logger.debug(f"Logged {operation} operation on {table_name} for row {row_id}")
    
    def get_changes_since(self, last_timestamp, batch_size=1000):
        """Get all changes since a given timestamp
        
        Args:
            last_timestamp: Timestamp to get changes since
            batch_size: Size of batches to fetch (to avoid memory issues)
            
        Returns:
            changes: List of changes since the timestamp
        """
        changes = []
        
        try:
            # Normalize the timestamp format to ensure consistent comparison
            # Handle various timestamp formats including those with timezone info
            try:
                dt = None
                # Handle timezone offsets like +00:00
                if '+' in last_timestamp and not last_timestamp.endswith('Z'):
                    # Split at the + and parse the main part
                    timestamp_parts = last_timestamp.split('+')
                    base_timestamp = timestamp_parts[0]
                    
                    # Parse the base timestamp
                    if '.' in base_timestamp:
                        dt = datetime.strptime(base_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                    else:
                        dt = datetime.strptime(base_timestamp, "%Y-%m-%dT%H:%M:%S")
                    
                    logger.debug(f"Parsed timestamp with timezone offset: {base_timestamp}")
                # Handle standard formats
                elif '.' in last_timestamp:
                    # If it has milliseconds, parse accordingly
                    dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                elif 'Z' in last_timestamp:
                    # If it has Z timezone indicator
                    dt = datetime.strptime(last_timestamp.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                else:
                    # Basic ISO format without timezone
                    dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S")
            except ValueError as e:
                logger.warning(f"Error parsing timestamp '{last_timestamp}': {e}")
                dt = datetime(1970, 1, 1, 0, 0, 0)  # Use epoch start if parsing fails
                
            # Convert back to a standard ISO format without timezone for consistent comparison
            normalized_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")
            
            logger.debug(f"Normalized timestamp from '{last_timestamp}' to '{normalized_timestamp}'")
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # First, get all changes from the change_log table
                cursor.execute('''
                    SELECT table_name, operation, row_id, timestamp, content, content_hash
                    FROM change_log
                    ORDER BY timestamp
                ''')
                
                all_changes = cursor.fetchall()
                
                # Filter changes that have a timestamp after our normalized_timestamp
                # by using timestamp comparison on parsed datetime objects
                filtered_changes = []
                for change in all_changes:
                    change_timestamp = change[3]  # timestamp is at index 3
                    
                    try:
                        # Parse the change timestamp with the same flexible logic
                        change_dt = None
                        
                        # Handle timezone offsets
                        if '+' in change_timestamp and not change_timestamp.endswith('Z'):
                            timestamp_parts = change_timestamp.split('+')
                            base_timestamp = timestamp_parts[0]
                            
                            if '.' in base_timestamp:
                                change_dt = datetime.strptime(base_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                            else:
                                change_dt = datetime.strptime(base_timestamp, "%Y-%m-%dT%H:%M:%S")
                        # Handle standard formats
                        elif '.' in change_timestamp:
                            change_dt = datetime.strptime(change_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                        elif 'Z' in change_timestamp:
                            change_dt = datetime.strptime(change_timestamp.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                        else:
                            change_dt = datetime.strptime(change_timestamp, "%Y-%m-%dT%H:%M:%S")
                            
                        # Compare with our timestamp
                        if change_dt > dt:
                            filtered_changes.append(change)
                            
                    except ValueError as e:
                        logger.warning(f"Failed to parse change timestamp '{change_timestamp}': {e}")
                        # Skip this change if we can't parse its timestamp
                
                changes = filtered_changes
                
                logger.debug(f"Found {len(changes)} changes since timestamp {last_timestamp}")
        except Exception as e:
            logger.error(f"Error getting changes since timestamp '{last_timestamp}': {e}")
            # If there was an error in timestamp parsing, fall back to returning an empty change set
        
        return changes
    
    def get_db_version(self):
        """Calculate database version based on content and get timestamp
        
        Returns:
            version: Dictionary with hash and timestamp
        """
        if not os.path.exists(self.db_manager.db_path):
            return {"hash": "0", "timestamp": "1970-01-01T00:00:00Z"}
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Calculate content-based hash from all tracked tables
                tables = ['beers', 'taps']
                content_hashes = []
                for table in tables:
                    content_hashes.append(self._get_table_hash(table))
                content_hash = hashlib.md5(''.join(content_hashes).encode()).hexdigest()
                
                # Get timestamp from version table
                try:
                    # Get timestamp and hash from version table
                    cursor.execute("SELECT timestamp, hash FROM version WHERE id = 1 LIMIT 1")
                    row = cursor.fetchone()
                    
                    if row:
                        timestamp, stored_hash = row
                        
                        # Always update the hash to ensure it's correct
                        # This guarantees the hash reflects the current database state
                        cursor.execute(
                            "UPDATE version SET hash = ? WHERE id = 1", 
                            (content_hash,)
                        )
                        conn.commit()
                    else:
                        timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                        cursor.execute(
                            "INSERT INTO version (id, timestamp, hash) VALUES (1, ?, ?)",
                            (timestamp, content_hash)
                        )
                        conn.commit()
                except sqlite3.Error as e:
                    logger.error(f"Error accessing version table: {e}")
                    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    # Try to create the version table with proper schema
                    try:
                        cursor.execute('''
                            CREATE TABLE IF NOT EXISTS version (
                                id INTEGER PRIMARY KEY,
                                timestamp TEXT NOT NULL,
                                hash TEXT NOT NULL
                            )
                        ''')
                        cursor.execute(
                            "INSERT INTO version (id, timestamp, hash) VALUES (1, ?, ?)",
                            (timestamp, content_hash)
                        )
                        conn.commit()
                    except sqlite3.Error as e2:
                        logger.error(f"Error creating version table: {e2}")
        except Exception as e:
            logger.error(f"Error getting database version: {e}")
            return {"hash": "0", "timestamp": "1970-01-01T00:00:00Z"}
            
        return {"hash": content_hash, "timestamp": timestamp}
    
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
                    
                    cursor.execute(f"SELECT * FROM {table_name} ORDER BY rowid")
                    rows = cursor.fetchall()
                    return hashlib.md5(str(rows).encode()).hexdigest()
                except sqlite3.Error as e:
                    logger.error(f"Error calculating hash for {table_name}: {e}")
                    return "0"
        except Exception as e:
            logger.error(f"Unexpected error in _get_table_hash: {e}")
            return "0"
    
    def prune_change_log(self, days_to_keep=30):
        """Prune old entries from the change_log table
        
        Args:
            days_to_keep: Number of days worth of changes to keep
            
        Returns:
            count: Number of entries pruned
        """
        from datetime import timedelta
        
        # Calculate the cutoff date
        cutoff_date = (datetime.now(UTC) - timedelta(days=days_to_keep)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get count of entries to be pruned
            cursor.execute("SELECT COUNT(*) FROM change_log WHERE timestamp < ?", (cutoff_date,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                # Delete old entries
                cursor.execute("DELETE FROM change_log WHERE timestamp < ?", (cutoff_date,))
                conn.commit()
                logger.info(f"Pruned {count} entries from change_log")
            
            return count
    
    def is_newer_version(self, version1, version2):
        """Determine if version1 is newer than version2 based on timestamps
        
        Args:
            version1: Version dict with hash and timestamp
            version2: Version dict with hash and timestamp
            
        Returns:
            is_newer: True if version1 is newer than version2
        """
        # If version2 is empty database, any other version is newer
        if version2.get("hash") == "0":
            return True
        
        # If hashes are the same, they are the same version
        if version1.get("hash") == version2.get("hash"):
            return False
            
        # Compare timestamps
        try:
            # Get timestamp strings, defaulting to epoch start if missing
            ts1_str = version1.get("timestamp", "1970-01-01T00:00:00Z")
            ts2_str = version2.get("timestamp", "1970-01-01T00:00:00Z")
            
            # Parse timestamps, handling different formats
            try:
                if '.' in ts1_str:
                    # Handle milliseconds format
                    timestamp1 = datetime.strptime(ts1_str, "%Y-%m-%dT%H:%M:%S.%f")
                elif 'Z' in ts1_str:
                    # Handle Z timezone format
                    timestamp1 = datetime.strptime(ts1_str.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                else:
                    # Handle basic format
                    timestamp1 = datetime.strptime(ts1_str, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # If parsing fails, default to epoch start
                logger.warning(f"Failed to parse timestamp1: {ts1_str}, using epoch start")
                timestamp1 = datetime(1970, 1, 1, 0, 0, 0)
                
            try:
                if '.' in ts2_str:
                    # Handle milliseconds format
                    timestamp2 = datetime.strptime(ts2_str, "%Y-%m-%dT%H:%M:%S.%f")
                elif 'Z' in ts2_str:
                    # Handle Z timezone format
                    timestamp2 = datetime.strptime(ts2_str.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                else:
                    # Handle basic format
                    timestamp2 = datetime.strptime(ts2_str, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # If parsing fails, default to epoch start
                logger.warning(f"Failed to parse timestamp2: {ts2_str}, using epoch start")
                timestamp2 = datetime(1970, 1, 1, 0, 0, 0)
            
            logger.debug(f"Comparing timestamps: {timestamp1} vs {timestamp2}")
            return timestamp1 > timestamp2
            
        except Exception as e:
            logger.error(f"Error comparing timestamps: {e}")
            # Fall back to hash comparison if timestamp comparison fails
            return version1.get("hash") != version2.get("hash") 