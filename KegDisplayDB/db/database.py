"""
Database management module for KegDisplay.
Handles core database operations for beer and tap management.
"""

import sqlite3
import logging
from datetime import datetime, UTC
import os
import json
import threading
import queue
import hashlib
import time
import uuid

logger = logging.getLogger("KegDisplay")

class DatabaseManager:
    """
    Handles core database operations for the KegDisplay system.
    Manages the beer and tap tables and provides CRUD operations.
    """
    
    # Class variable to store connection pools for different database paths
    _connection_pools = {}
    _pool_locks = {}
    
    def __init__(self, db_path, pool_size=5):
        """Initialize the database manager
        
        Args:
            db_path: Path to the SQLite database file
            pool_size: Size of the connection pool
        """
        self.db_path = db_path
        self.pool_size = pool_size
        
        self._initialize_connection_pool()
        self.initialize_tables()
    
    def _initialize_connection_pool(self):
        """Initialize the connection pool for this database path"""
        
        # Create a pool lock if it doesn't exist
        if self.db_path not in self._pool_locks:
            self._pool_locks[self.db_path] = threading.Lock()
        
        # Create a connection pool if it doesn't exist
        with self._pool_locks[self.db_path]:
            if self.db_path not in self._connection_pools:
                self._connection_pools[self.db_path] = queue.Queue(maxsize=self.pool_size)
                
                # Pre-populate the pool with connections
                for _ in range(self.pool_size):
                    conn = sqlite3.connect(self.db_path, check_same_thread=False)
                    self._connection_pools[self.db_path].put(conn)
    
    def initialize_tables(self):
        """Initialize database tables if they don't exist"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create beers table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS beers (
                    idBeer INTEGER PRIMARY KEY,
                    Name tinytext NOT NULL,
                    ABV float,
                    IBU float,
                    Color float,
                    OriginalGravity float,
                    FinalGravity float,
                    Description TEXT,
                    Brewed datetime,
                    Kegged datetime,
                    Tapped datetime,
                    Notes TEXT
                )
            ''')
            
            # Create taps table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS taps (
                    idTap INTEGER PRIMARY KEY,
                    idBeer INTEGER
                )
            ''')
            
            # Create change_log table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS change_log (
                    id INTEGER PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    row_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    content TEXT,
                    content_hash TEXT,
                    logical_clock INTEGER DEFAULT 0,
                    node_id TEXT
                )
            ''')
            
            # Create version table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS version (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    logical_clock INTEGER DEFAULT 0,
                    node_id TEXT
                )
            ''')
            
            conn.commit()
            logger.info("Database tables initialized")
    
    def get_connection(self):
        """Get a database connection from the pool or create a new one
        
        Returns:
            ConnectionContext: Context manager for the connection
        """
        # Use a context manager to ensure connections are returned to the pool
        class ConnectionContext:
            def __init__(self, db_manager, conn):
                self.db_manager = db_manager
                self.conn = conn
            
            def __enter__(self):
                return self.conn
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                # Return connection to the pool instead of closing it
                try:
                    if self.conn:
                        # Rollback any uncommitted changes if there was an exception
                        if exc_type:
                            self.conn.rollback()
                        self.db_manager._connection_pools[self.db_manager.db_path].put(self.conn)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
                    # If there's an error returning to the pool, close it
                    if self.conn:
                        self.conn.close()

        try:
            if self.db_path in self._connection_pools:
                with self._pool_locks[self.db_path]:
                    try:
                        conn = self._connection_pools[self.db_path].get(block=False)
                    except queue.Empty:
                        # If pool is empty, create a new connection
                        conn = sqlite3.connect(self.db_path, check_same_thread=False)
                
                return ConnectionContext(self, conn)
        except Exception as e:
            logger.error(f"Error getting connection from pool: {e}")
            # If there's an error with the pool, fall back to a direct connection
            return sqlite3.connect(self.db_path, check_same_thread=False)
            
    def close_all_connections(self):
        """Close all connections in the pool for this database path"""
        if self.db_path in self._connection_pools:
            with self._pool_locks[self.db_path]:
                # Empty the queue and close all connections
                while not self._connection_pools[self.db_path].empty():
                    try:
                        conn = self._connection_pools[self.db_path].get(block=False)
                        conn.close()
                    except Exception as e:
                        logger.error(f"Error closing connection: {e}")
    
    def __del__(self):
        """Clean up resources when the instance is destroyed"""
        try:
            self.close_all_connections()
        except:
            pass
    
    def query(self, sql, params=(), fetch_all=False, row_factory=None):
        """Execute a query and return results
        
        Args:
            sql: SQL query string
            params: Parameters for the query
            fetch_all: Whether to fetch all results or just one
            row_factory: Optional row factory to use for result rows
            
        Returns:
            Result of the query execution
        """
        with self.get_connection() as conn:
            if row_factory:
                conn.row_factory = row_factory
            cursor = conn.cursor()
            cursor.execute(sql, params)
            
            if sql.strip().upper().startswith(("SELECT", "PRAGMA")):
                if fetch_all:
                    return cursor.fetchall()
                else:
                    return cursor.fetchone()
            else:
                conn.commit()
                return cursor.lastrowid if cursor.lastrowid else cursor.rowcount
    
    # ---- Beer Management Methods ----
    
    def add_beer(self, name, abv=None, ibu=None, color=None, og=None, fg=None, 
                description=None, brewed=None, kegged=None, tapped=None, notes=None, conn=None):
        """Add a new beer to the database
        
        Args:
            name: Beer name
            abv: Alcohol by volume percentage
            ibu: International bitterness units
            color: SRM color
            og: Original gravity
            fg: Final gravity
            description: Beer description
            brewed: Brew date (datetime object or string)
            kegged: Keg date (datetime object or string)
            tapped: Tap date (datetime object or string)
            notes: Additional notes
            conn: Optional database connection to use (to avoid nested transactions)
            
        Returns:
            id: The ID of the newly added beer
        """
        # Convert datetime objects to strings if needed
        if isinstance(brewed, datetime):
            # Ensure datetime is in UTC
            if getattr(brewed, 'tzinfo', None) is None:
                brewed = brewed.replace(tzinfo=UTC)
            brewed = brewed.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(kegged, datetime):
            # Ensure datetime is in UTC
            if getattr(kegged, 'tzinfo', None) is None:
                kegged = kegged.replace(tzinfo=UTC)
            kegged = kegged.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(tapped, datetime):
            # Ensure datetime is in UTC
            if getattr(tapped, 'tzinfo', None) is None:
                tapped = tapped.replace(tzinfo=UTC)
            tapped = tapped.strftime("%Y-%m-%d %H:%M:%S")
        
        # If a connection was provided, use it directly
        if conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO beers (
                    Name, ABV, IBU, Color, OriginalGravity, FinalGravity,
                    Description, Brewed, Kegged, Tapped, Notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, abv, ibu, color, og, fg, description, brewed, kegged, tapped, notes))
            
            beer_id = cursor.lastrowid
            # Do not commit - caller will handle this
            
            logger.info(f"Added beer '{name}' with ID {beer_id}")
            return beer_id
        else:
            # Use our own connection if none was provided
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO beers (
                        Name, ABV, IBU, Color, OriginalGravity, FinalGravity,
                        Description, Brewed, Kegged, Tapped, Notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (name, abv, ibu, color, og, fg, description, brewed, kegged, tapped, notes))
                
                beer_id = cursor.lastrowid
                conn.commit()
                
                logger.info(f"Added beer '{name}' with ID {beer_id}")
                return beer_id
    
    def update_beer(self, beer_id, name=None, abv=None, ibu=None, color=None, og=None, fg=None,
                   description=None, brewed=None, kegged=None, tapped=None, notes=None, conn=None):
        """Update an existing beer in the database
        
        Args:
            beer_id: ID of the beer to update
            name: New beer name (optional)
            abv: New alcohol by volume percentage (optional)
            ibu: New international bitterness units (optional)
            color: New SRM color (optional)
            og: New original gravity (optional)
            fg: New final gravity (optional)
            description: New beer description (optional)
            brewed: New brew date (datetime object or string) (optional)
            kegged: New keg date (datetime object or string) (optional)
            tapped: New tap date (datetime object or string) (optional)
            notes: New additional notes (optional)
            conn: Optional database connection to use (to avoid nested transactions)
            
        Returns:
            success: Whether the update was successful
        """
        # First check if the beer exists
        if not self.get_beer(beer_id):
            logger.error(f"Cannot update beer with ID {beer_id}: Beer not found")
            return False
        
        # Convert datetime objects to strings if needed
        if isinstance(brewed, datetime):
            # Ensure datetime is in UTC
            if getattr(brewed, 'tzinfo', None) is None:
                brewed = brewed.replace(tzinfo=UTC)
            brewed = brewed.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(kegged, datetime):
            # Ensure datetime is in UTC
            if getattr(kegged, 'tzinfo', None) is None:
                kegged = kegged.replace(tzinfo=UTC)
            kegged = kegged.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(tapped, datetime):
            # Ensure datetime is in UTC
            if getattr(tapped, 'tzinfo', None) is None:
                tapped = tapped.replace(tzinfo=UTC)
            tapped = tapped.strftime("%Y-%m-%d %H:%M:%S")
            
        # If a connection was provided, use it directly
        if conn:
            # Get existing data for any fields not specified
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
            existing_beer = cursor.fetchone()
            
            if not existing_beer:
                logger.error(f"Cannot update beer with ID {beer_id}: Beer not found")
                return False
                
            # Update only the fields that were specified
            update_name = name if name is not None else existing_beer[1]
            update_abv = abv if abv is not None else existing_beer[2]
            update_ibu = ibu if ibu is not None else existing_beer[3]
            update_color = color if color is not None else existing_beer[4]
            update_og = og if og is not None else existing_beer[5]
            update_fg = fg if fg is not None else existing_beer[6]
            update_description = description if description is not None else existing_beer[7]
            update_brewed = brewed if brewed is not None else existing_beer[8]
            update_kegged = kegged if kegged is not None else existing_beer[9]
            update_tapped = tapped if tapped is not None else existing_beer[10]
            update_notes = notes if notes is not None else existing_beer[11]
            
            # Update the database
            cursor.execute('''
                UPDATE beers SET
                    Name = ?, ABV = ?, IBU = ?, Color = ?, OriginalGravity = ?, FinalGravity = ?,
                    Description = ?, Brewed = ?, Kegged = ?, Tapped = ?, Notes = ?
                WHERE idBeer = ?
            ''', (
                update_name, update_abv, update_ibu, update_color, update_og, update_fg,
                update_description, update_brewed, update_kegged, update_tapped, update_notes,
                beer_id
            ))
            
            # Do not commit - caller will handle this
            logger.info(f"Updated beer {beer_id} with name '{update_name}'")
            return True
        else:
            # Use our own connection if none was provided
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
                existing_beer = cursor.fetchone()
                
                if not existing_beer:
                    logger.error(f"Cannot update beer with ID {beer_id}: Beer not found")
                    return False
                    
                # Update only the fields that were specified
                update_name = name if name is not None else existing_beer[1]
                update_abv = abv if abv is not None else existing_beer[2]
                update_ibu = ibu if ibu is not None else existing_beer[3]
                update_color = color if color is not None else existing_beer[4]
                update_og = og if og is not None else existing_beer[5]
                update_fg = fg if fg is not None else existing_beer[6]
                update_description = description if description is not None else existing_beer[7]
                update_brewed = brewed if brewed is not None else existing_beer[8]
                update_kegged = kegged if kegged is not None else existing_beer[9]
                update_tapped = tapped if tapped is not None else existing_beer[10]
                update_notes = notes if notes is not None else existing_beer[11]
                
                # Update the database
                cursor.execute('''
                    UPDATE beers SET
                        Name = ?, ABV = ?, IBU = ?, Color = ?, OriginalGravity = ?, FinalGravity = ?,
                        Description = ?, Brewed = ?, Kegged = ?, Tapped = ?, Notes = ?
                    WHERE idBeer = ?
                ''', (
                    update_name, update_abv, update_ibu, update_color, update_og, update_fg,
                    update_description, update_brewed, update_kegged, update_tapped, update_notes,
                    beer_id
                ))
                
                conn.commit()
                logger.info(f"Updated beer {beer_id} with name '{update_name}'")
                return True
    
    def delete_beer(self, beer_id):
        """Delete a beer from the database
        
        Args:
            beer_id: ID of the beer to delete
            
        Returns:
            success: True if the beer was deleted, False if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # First check if beer exists
            cursor.execute("SELECT Name FROM beers WHERE idBeer = ?", (beer_id,))
            beer = cursor.fetchone()
            
            if not beer:
                logger.warning(f"Beer with ID {beer_id} not found for deletion")
                return False
            
            beer_name = beer[0]
            
            # Delete the beer
            cursor.execute("DELETE FROM beers WHERE idBeer = ?", (beer_id,))
            conn.commit()
            
            if cursor.rowcount > 0:
                logger.info(f"Deleted beer '{beer_name}' with ID {beer_id}")
                return True
            else:
                logger.warning(f"Failed to delete beer with ID {beer_id}")
                return False
    
    def get_beer(self, beer_id):
        """Get a beer by ID
        
        Args:
            beer_id: ID of the beer to retrieve
            
        Returns:
            beer: Dictionary with beer information or None if not found
        """
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT * FROM beers WHERE idBeer = ?", 
                (beer_id,)
            )
            
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            else:
                return None
    
    def get_all_beers(self):
        """Get all beers from the database
        
        Returns:
            beers: List of dictionaries with beer information
        """
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM beers ORDER BY Name")
            
            return [dict(row) for row in cursor.fetchall()]
    
    # ---- Tap Management Methods ----
    
    def add_tap(self, tap_id=None, beer_id=None):
        """Add a new tap to the database
        
        Args:
            tap_id: ID for the tap (optional, auto-generated if not provided)
            beer_id: ID of the beer to assign (optional)
            
        Returns:
            id: ID of the newly added tap
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if tap_id:
                # Check if tap with this ID already exists
                cursor.execute("SELECT COUNT(*) FROM taps WHERE idTap = ?", (tap_id,))
                if cursor.fetchone()[0] > 0:
                    logger.warning(f"Tap with ID {tap_id} already exists")
                    return None
                
                # Insert with specified ID
                cursor.execute(
                    "INSERT INTO taps (idTap, idBeer) VALUES (?, ?)",
                    (tap_id, beer_id)
                )
            else:
                # Auto-generate ID
                cursor.execute(
                    "INSERT INTO taps (idBeer) VALUES (?)",
                    (beer_id,)
                )
            
            tap_id = cursor.lastrowid
            conn.commit()
            
            logger.info(f"Added tap {tap_id} with beer ID {beer_id}")
            return tap_id
    
    def update_tap(self, tap_id, beer_id):
        """Update a tap's beer assignment
        
        Args:
            tap_id: ID of the tap to update
            beer_id: ID of the beer to assign (None to unassign)
            
        Returns:
            success: True if the tap was updated, False if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if tap exists
            cursor.execute("SELECT idTap FROM taps WHERE idTap = ?", (tap_id,))
            if not cursor.fetchone():
                logger.warning(f"Tap with ID {tap_id} not found for update")
                return False
            
            # Update the tap
            cursor.execute("UPDATE taps SET idBeer = ? WHERE idTap = ?", (beer_id, tap_id))
            conn.commit()
            
            logger.info(f"Updated tap {tap_id} with beer ID {beer_id}")
            return True
    
    def delete_tap(self, tap_id):
        """Delete a tap from the database
        
        Args:
            tap_id: ID of the tap to delete
            
        Returns:
            success: True if deleted, False if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if tap exists
            cursor.execute("SELECT idTap FROM taps WHERE idTap = ?", (tap_id,))
            if not cursor.fetchone():
                logger.warning(f"Tap with ID {tap_id} not found for deletion")
                return False
            
            # Delete the tap
            cursor.execute("DELETE FROM taps WHERE idTap = ?", (tap_id,))
            conn.commit()
            
            logger.info(f"Deleted tap {tap_id}")
            return True
    
    def get_tap(self, tap_id):
        """Get a tap by ID
        
        Args:
            tap_id: ID of the tap to retrieve
            
        Returns:
            tap: Dictionary with tap information or None if not found
        """
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT t.*, b.Name as BeerName FROM taps t "
                "LEFT JOIN beers b ON t.idBeer = b.idBeer "
                "WHERE t.idTap = ?", 
                (tap_id,)
            )
            
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            else:
                return None
    
    def get_all_taps(self):
        """Get all taps with their beer information
        
        Returns:
            taps: List of dictionaries with tap information
        """
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT t.*, b.Name as BeerName FROM taps t "
                "LEFT JOIN beers b ON t.idBeer = b.idBeer "
                "ORDER BY t.idTap"
            )
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_tap_with_beer(self, beer_id):
        """Get IDs of taps that have a specific beer assigned
        
        Args:
            beer_id: ID of the beer to look for
            
        Returns:
            list: List of tap IDs that have the beer assigned
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT idTap FROM taps WHERE idBeer = ?",
                (beer_id,)
            )
            
            return [row[0] for row in cursor.fetchall()]
            
    def apply_sync_changes(self, changes):
        """Apply changes received during sync
        
        Args:
            changes: List of changes to apply
            
        Note:
            This implements the Lamport Clock rule for receiving sync responses:
            For each record (op, t, origin) in clock-ordered stream:
            1. If unseen:
               a. localClock = max(localClock, t) + 1
               b. version_table.clock = localClock
               c. db.apply(op)
               d. change_log.insert({op, t, origin})
            2. Otherwise skip
        """
        if not changes:
            logger.info("No changes to apply")
            return
            
        logger.info(f"Applying {len(changes)} sync changes")
        applied_changes = 0
        failed_changes = 0
            
        try:
            # Start transaction
            with self.get_connection() as conn:
                conn.execute('BEGIN TRANSACTION')
               
                # Track the highest logical clock from applied changes
                highest_logical_clock = 0
                peer_node_id = None
                
                # Get current logical clock
                cursor = conn.cursor()
                cursor.execute("SELECT logical_clock FROM version WHERE id = 1")
                row = cursor.fetchone()
                current_clock = row[0] if row and row[0] is not None else 0
                
                # Process each change in clock-ordered stream
                for change_index, change in enumerate(changes):
                    try:
                        # Ensure the change has all the required fields
                        if len(change) < 6:
                            logger.warning(f"Change at index {change_index} is missing required fields: {change}")
                            failed_changes += 1
                            continue
                        
                        # Extract change information
                        table_name = change[0]
                        operation = change[1]
                        row_id = change[2]
                        timestamp = change[3]
                        content = change[4]
                        content_hash = change[5]
                        
                        # Extract logical clock and node_id if present
                        logical_clock = 0
                        node_id = None
                        if len(change) >= 7:
                            logical_clock = change[6]
                        if len(change) >= 8:
                            node_id = change[7]
                            # Store peer node ID for version table update
                            if not peer_node_id:
                                peer_node_id = node_id
                        
                        # Check if this change is already in our change_log
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM change_log 
                            WHERE table_name = ? AND operation = ? AND row_id = ? AND logical_clock = ? AND node_id = ?
                            """,
                            (table_name, operation, row_id, logical_clock, node_id)
                        )
                        count = cursor.fetchone()[0]
                        
                        if count > 0:
                            # Skip changes we've already processed
                            logger.debug(f"Skipping already applied change: {operation} on {table_name}.{row_id}")
                            continue
                            
                        # Verify content hash (security check)
                        if hashlib.md5(content.encode()).hexdigest() != content_hash:
                            logger.warning(f"Content hash mismatch for change at index {change_index}")
                            failed_changes += 1
                            continue
                        
                        # Update our logical clock (Lamport rule: localClock = max(localClock, t) + 1)
                        new_clock = max(current_clock, logical_clock) + 1
                        current_clock = new_clock  # Update for next iteration
                        
                        # Track the highest computed clock
                        if new_clock > highest_logical_clock:
                            highest_logical_clock = new_clock
                        
                        logger.debug(f"Applying change: {operation} to {table_name}.{row_id} (logical clock: {logical_clock}, our new clock: {new_clock})")
                        
                        # Apply the change based on operation type
                        if operation == 'INSERT' or operation == 'UPDATE':
                            try:
                                # Parse the content as JSON and build the SQL
                                import json
                                row_data = json.loads(content)
                                
                                if operation == 'INSERT':
                                    # Build INSERT statement
                                    columns = ', '.join(row_data.keys())
                                    placeholders = ', '.join(['?'] * len(row_data))
                                    sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
                                    conn.execute(sql, list(row_data.values()))
                                    
                                elif operation == 'UPDATE':
                                    # Build UPDATE statement
                                    set_clause = ', '.join([f"{col} = ?" for col in row_data.keys()])
                                    sql = f"UPDATE {table_name} SET {set_clause} WHERE rowid = ?"
                                    params = list(row_data.values()) + [row_id]
                                    conn.execute(sql, params)
                                
                                # Log the change in our change_log table with OUR new clock value
                                conn.execute(
                                    """
                                    INSERT INTO change_log 
                                    (table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id) 
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (table_name, operation, row_id, timestamp, content, content_hash, new_clock, node_id)
                                )
                                applied_changes += 1
                                
                            except Exception as e:
                                logger.error(f"Error applying {operation} change to {table_name}.{row_id}: {e}")
                                failed_changes += 1
                                
                        elif operation == 'DELETE':
                            try:
                                # Execute DELETE statement
                                sql = f"DELETE FROM {table_name} WHERE rowid = ?"
                                conn.execute(sql, (row_id,))
                                
                                # Log the change in our change_log table with OUR new clock value
                                conn.execute(
                                    """
                                    INSERT INTO change_log 
                                    (table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id) 
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (table_name, operation, row_id, timestamp, content, content_hash, new_clock, node_id)
                                )
                                applied_changes += 1
                                
                            except Exception as e:
                                logger.error(f"Error applying DELETE change to {table_name}.{row_id}: {e}")
                                failed_changes += 1
                        else:
                            logger.warning(f"Unknown operation '{operation}' in change at index {change_index}")
                            failed_changes += 1
                            
                    except Exception as e:
                        logger.error(f"Error processing change at index {change_index}: {e}")
                        failed_changes += 1
                
                # Update the version table with the highest computed logical clock
                if highest_logical_clock > 0:
                    # Update version table with new logical clock and timestamp
                    timestamp = datetime.now(UTC).isoformat()
                    conn.execute(
                        "UPDATE version SET timestamp = ?, logical_clock = ? WHERE id = 1",
                        (timestamp, highest_logical_clock)
                    )
                    
                    # If no row was updated, insert one
                    if conn.total_changes == 0:
                        # Calculate current database hash
                        tables = ['beers', 'taps']
                        content_hash = self._calculate_db_hash(tables, cursor)
                        
                        cursor.execute(
                            "INSERT INTO version (timestamp, hash, logical_clock, node_id) VALUES (?, ?, ?, ?)",
                            (timestamp, content_hash, highest_logical_clock, peer_node_id)
                        )
                    
                    logger.info(f"Updated version with logical clock {highest_logical_clock}")
                
                conn.commit()
                logger.info(f"Successfully applied {applied_changes} changes, {failed_changes} failed")
                
        except Exception as e:
            logger.error(f"Error applying sync changes: {e}")
            return False
        
        return applied_changes > 0
    
    def import_from_file(self, temp_db_path):
        """Import the entire database from a file without replacing the original
        
        Args:
            temp_db_path: Path to the database file to import from
            
        Returns:
            success: Whether the import was successful
        """
        logger.info(f"Importing database from {temp_db_path}")
        
        try:
            # Connect to both databases
            with sqlite3.connect(self.db_path) as main_conn, sqlite3.connect(temp_db_path) as temp_conn:
                main_cursor = main_conn.cursor()
                temp_cursor = temp_conn.cursor()
                
                # Begin transaction
                main_conn.execute("BEGIN TRANSACTION")
                
                # Clear existing tables
                main_cursor.execute("DELETE FROM beers")
                main_cursor.execute("DELETE FROM taps")
                
                # Get all beers from temp db
                temp_cursor.execute("SELECT * FROM beers")
                beers = temp_cursor.fetchall()
                
                # Insert beers into main db
                if beers:
                    beer_columns = [d[0] for d in temp_cursor.description]
                    beer_placeholders = ", ".join(["?"] * len(beer_columns))
                    beer_insert_sql = f"INSERT INTO beers ({', '.join(beer_columns)}) VALUES ({beer_placeholders})"
                    
                    for beer in beers:
                        main_cursor.execute(beer_insert_sql, beer)
                
                # Get all taps from temp db
                temp_cursor.execute("SELECT * FROM taps")
                taps = temp_cursor.fetchall()
                
                # Insert taps into main db
                if taps:
                    tap_columns = [d[0] for d in temp_cursor.description]
                    tap_placeholders = ", ".join(["?"] * len(tap_columns))
                    tap_insert_sql = f"INSERT INTO taps ({', '.join(tap_columns)}) VALUES ({tap_placeholders})"
                    
                    for tap in taps:
                        main_cursor.execute(tap_insert_sql, tap)
                
                # Get the version information from the temp database
                try:
                    # Get our local node_id
                    main_cursor.execute("SELECT node_id FROM version WHERE id = 1")
                    local_node_id_row = main_cursor.fetchone()
                    local_node_id = local_node_id_row[0] if local_node_id_row else None
                    
                    if not local_node_id:
                        # If we don't have a node_id, generate one
                        local_node_id = str(uuid.uuid4())
                        logger.info(f"Generated new node_id for import: {local_node_id}")
                    
                    # Check if version table exists and has logical_clock column
                    temp_cursor.execute("PRAGMA table_info(version)")
                    version_columns = [col[1] for col in temp_cursor.fetchall()]
                    
                    if 'logical_clock' in version_columns:
                        temp_cursor.execute("SELECT timestamp, hash, logical_clock, node_id FROM version WHERE id = 1")
                        version_row = temp_cursor.fetchone()
                        if version_row:
                            timestamp, db_hash, logical_clock, node_id = version_row
                            
                            # Preserve the imported logical clock value
                            logger.info(f"Importing version data with logical_clock {logical_clock}")
                            
                            # Check if version table exists in our DB
                            main_cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='version'")
                            if main_cursor.fetchone()[0] > 0:
                                # Update our version table with the imported values but keep our node_id
                                main_cursor.execute("""
                                    UPDATE version 
                                    SET timestamp = ?, hash = ?, logical_clock = ?
                                    WHERE id = 1
                                """, (timestamp, db_hash, logical_clock))
                            else:
                                # Create version record if it doesn't exist
                                main_cursor.execute("""
                                    INSERT INTO version (id, timestamp, hash, logical_clock, node_id)
                                    VALUES (1, ?, ?, ?, ?)
                                """, (timestamp, db_hash, logical_clock, local_node_id))
                except Exception as e:
                    logger.warning(f"Could not import version information: {e}")
                
                # Commit transaction
                main_conn.commit()
                logger.info(f"Successfully imported database with {len(beers)} beers and {len(taps)} taps")
                return True
                
        except Exception as e:
            logger.error(f"Error importing database: {e}")
            return False
    
    def _apply_insert_change(self, conn, table_name, row_id, content_data):
        """Apply an INSERT change to the database
        
        Args:
            conn: Database connection
            table_name: Name of the table
            row_id: ID of the row
            content_data: Content data (list or dict)
        """
        try:
            logger.debug(f"Applying INSERT to {table_name}.{row_id}")
            logger.debug(f"Content data type: {type(content_data)}")
            
            if table_name == 'beers':
                if isinstance(content_data, list):
                    # Map values from list to appropriate column positions
                    # Assuming format is [idBeer, Name, ABV, IBU, Color, OriginalGravity, FinalGravity, Description, etc.]
                    logger.debug(f"INSERT beer with list data: {content_data}")
                    conn.execute('''
                        INSERT OR REPLACE INTO beers (idBeer, Name, ABV, IBU, Color, OriginalGravity, 
                        FinalGravity, Description, Brewed, Kegged, Tapped, Notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row_id, content_data[1] if len(content_data) > 1 else None, 
                        content_data[2] if len(content_data) > 2 else None,
                        content_data[3] if len(content_data) > 3 else None, 
                        content_data[4] if len(content_data) > 4 else None,
                        content_data[5] if len(content_data) > 5 else None,
                        content_data[6] if len(content_data) > 6 else None,
                        content_data[7] if len(content_data) > 7 else None,
                        content_data[8] if len(content_data) > 8 else None,
                        content_data[9] if len(content_data) > 9 else None,
                        content_data[10] if len(content_data) > 10 else None,
                        content_data[11] if len(content_data) > 11 else None))
                else:
                    # It's a dictionary, use .get()
                    # Log the dictionary keys to help with debugging
                    logger.debug(f"INSERT beer with dict data. Keys: {list(content_data.keys())}")
                    
                    # Check for different possible name conventions in the dictionary
                    name = None
                    if 'name' in content_data:
                        name = content_data.get('name')
                    elif 'Name' in content_data:
                        name = content_data.get('Name')
                        
                    # Same for other fields
                    abv = content_data.get('abv') or content_data.get('ABV')
                    ibu = content_data.get('ibu') or content_data.get('IBU')
                    color = content_data.get('color') or content_data.get('Color')
                    og = content_data.get('og') or content_data.get('OriginalGravity')
                    fg = content_data.get('fg') or content_data.get('FinalGravity')
                    description = content_data.get('description') or content_data.get('Description')
                    brewed = content_data.get('brewed') or content_data.get('Brewed')
                    kegged = content_data.get('kegged') or content_data.get('Kegged')
                    tapped = content_data.get('tapped') or content_data.get('Tapped')
                    notes = content_data.get('notes') or content_data.get('Notes')
                    
                    logger.debug(f"Inserting beer: id={row_id}, name={name}, abv={abv}")
                    
                    conn.execute('''
                        INSERT OR REPLACE INTO beers (idBeer, Name, ABV, IBU, Color, OriginalGravity, 
                        FinalGravity, Description, Brewed, Kegged, Tapped, Notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row_id, name, abv, ibu, color, og, fg, description, brewed, kegged, tapped, notes))
            elif table_name == 'taps':
                if isinstance(content_data, list):
                    logger.debug(f"INSERT tap with list data: {content_data}")
                    conn.execute('''
                        INSERT OR REPLACE INTO taps (idTap, idBeer)
                        VALUES (?, ?)
                    ''', (row_id, content_data[1] if len(content_data) > 1 else None))
                else:
                    # It's a dictionary, use .get()
                    logger.debug(f"INSERT tap with dict data. Keys: {list(content_data.keys())}")
                    
                    # Check for different possible beer_id naming conventions
                    beer_id = None
                    if 'beer_id' in content_data:
                        beer_id = content_data.get('beer_id')
                    elif 'idBeer' in content_data:
                        beer_id = content_data.get('idBeer')
                    
                    logger.debug(f"Inserting tap: idTap={row_id}, idBeer={beer_id}")
                    
                    conn.execute('''
                        INSERT OR REPLACE INTO taps (idTap, idBeer)
                        VALUES (?, ?)
                    ''', (row_id, beer_id))
            else:
                logger.warning(f"Unknown table for INSERT: {table_name}")
                
        except sqlite3.Error as e:
            logger.error(f"SQLite error applying INSERT to {table_name}.{row_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error applying INSERT to {table_name}.{row_id}: {e}")
            raise
                
    def _apply_update_change(self, conn, table_name, row_id, content_data):
        """Apply an UPDATE change to the database
        
        Args:
            conn: Database connection
            table_name: Name of the table
            row_id: ID of the row
            content_data: Content data (list or dict)
        """
        try:
            logger.debug(f"Applying UPDATE to {table_name}.{row_id}")
            logger.debug(f"Content data type: {type(content_data)}")
            
            if table_name == 'beers':
                if isinstance(content_data, list):
                    # Map values from list to appropriate column positions
                    logger.debug(f"UPDATE beer with list data: {content_data}")
                    conn.execute('''
                        UPDATE beers SET Name=?, ABV=?, IBU=?, Color=?, OriginalGravity=?, 
                        FinalGravity=?, Description=?, Brewed=?, Kegged=?, Tapped=?, Notes=?
                        WHERE idBeer=?
                    ''', (content_data[1] if len(content_data) > 1 else None, 
                        content_data[2] if len(content_data) > 2 else None,
                        content_data[3] if len(content_data) > 3 else None, 
                        content_data[4] if len(content_data) > 4 else None,
                        content_data[5] if len(content_data) > 5 else None,
                        content_data[6] if len(content_data) > 6 else None,
                        content_data[7] if len(content_data) > 7 else None,
                        content_data[8] if len(content_data) > 8 else None,
                        content_data[9] if len(content_data) > 9 else None,
                        content_data[10] if len(content_data) > 10 else None,
                        content_data[11] if len(content_data) > 11 else None,
                        row_id))
                else:
                    # It's a dictionary, use .get()
                    # Log the dictionary keys to help with debugging
                    logger.debug(f"UPDATE beer with dict data. Keys: {list(content_data.keys())}")
                    
                    # Check for different possible name conventions in the dictionary
                    name = None
                    if 'name' in content_data:
                        name = content_data.get('name')
                    elif 'Name' in content_data:
                        name = content_data.get('Name')
                        
                    # Same for other fields
                    abv = content_data.get('abv') or content_data.get('ABV')
                    ibu = content_data.get('ibu') or content_data.get('IBU')
                    color = content_data.get('color') or content_data.get('Color')
                    og = content_data.get('og') or content_data.get('OriginalGravity')
                    fg = content_data.get('fg') or content_data.get('FinalGravity')
                    description = content_data.get('description') or content_data.get('Description')
                    brewed = content_data.get('brewed') or content_data.get('Brewed')
                    kegged = content_data.get('kegged') or content_data.get('Kegged')
                    tapped = content_data.get('tapped') or content_data.get('Tapped')
                    notes = content_data.get('notes') or content_data.get('Notes')
                    
                    logger.debug(f"Updating beer: id={row_id}, name={name}, abv={abv}")
                    
                    conn.execute('''
                        UPDATE beers SET Name=?, ABV=?, IBU=?, Color=?, OriginalGravity=?, 
                        FinalGravity=?, Description=?, Brewed=?, Kegged=?, Tapped=?, Notes=?
                        WHERE idBeer=?
                    ''', (name, abv, ibu, color, og, fg, description, brewed, kegged, tapped, notes, row_id))
            elif table_name == 'taps':
                if isinstance(content_data, list):
                    logger.debug(f"UPDATE tap with list data: {content_data}")
                    conn.execute('''
                        UPDATE taps SET idBeer=?
                        WHERE idTap=?
                    ''', (content_data[1] if len(content_data) > 1 else None, row_id))
                else:
                    # It's a dictionary, use .get()
                    logger.debug(f"UPDATE tap with dict data. Keys: {list(content_data.keys())}")
                    
                    # Check for different possible beer_id naming conventions
                    beer_id = None
                    if 'beer_id' in content_data:
                        beer_id = content_data.get('beer_id')
                    elif 'idBeer' in content_data:
                        beer_id = content_data.get('idBeer')
                    
                    logger.debug(f"Updating tap: idTap={row_id}, idBeer={beer_id}")
                    
                    conn.execute('''
                        UPDATE taps SET idBeer=?
                        WHERE idTap=?
                    ''', (beer_id, row_id))
            else:
                logger.warning(f"Unknown table for UPDATE: {table_name}")
                
        except sqlite3.Error as e:
            logger.error(f"SQLite error applying UPDATE to {table_name}.{row_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error applying UPDATE to {table_name}.{row_id}: {e}")
            raise
    
    def _apply_delete_change(self, conn, table_name, row_id):
        """Apply a DELETE change to the database
        
        Args:
            conn: Database connection
            table_name: Name of the table
            row_id: ID of the row
        """
        try:
            logger.debug(f"Applying DELETE to {table_name}.{row_id}")
            
            if table_name == 'beers':
                # Check if the row actually exists
                cursor = conn.cursor()
                cursor.execute('SELECT 1 FROM beers WHERE idBeer=?', (row_id,))
                if cursor.fetchone():
                    conn.execute('DELETE FROM beers WHERE idBeer=?', (row_id,))
                    logger.info(f"Deleted beer with idBeer={row_id}")
                else:
                    logger.warning(f"Beer with idBeer={row_id} not found for deletion")
            elif table_name == 'taps':
                # Check if the row actually exists
                cursor = conn.cursor()
                cursor.execute('SELECT 1 FROM taps WHERE idTap=?', (row_id,))
                if cursor.fetchone():
                    conn.execute('DELETE FROM taps WHERE idTap=?', (row_id,))
                    logger.info(f"Deleted tap with idTap={row_id}")
                else:
                    logger.warning(f"Tap with idTap={row_id} not found for deletion")
            else:
                logger.warning(f"Unknown table for DELETE: {table_name}")
                
        except sqlite3.Error as e:
            logger.error(f"SQLite error applying DELETE to {table_name}.{row_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error applying DELETE to {table_name}.{row_id}: {e}")
            raise 

    def _calculate_db_hash(self, tables, cursor):
        """Calculate a hash based on database content for version tracking
        
        Args:
            tables: List of table names to include in the hash
            cursor: Database cursor to use
            
        Returns:
            str: MD5 hash of relevant database content
        """
        try:
            # Calculate content-based hash from all tracked tables
            content_hashes = []
            for table in tables:
                # Get all rows from the table for hashing
                try:
                    cursor.execute(f"SELECT * FROM {table}")
                    rows = cursor.fetchall()
                    
                    # Get column names
                    cursor.execute(f"PRAGMA table_info({table})")
                    column_info = cursor.fetchall()
                    column_names = [col[1] for col in column_info]
                    
                    # Create normalized representation
                    normalized_data = []
                    for row in rows:
                        row_dict = {}
                        for i, col_name in enumerate(column_names):
                            val = row[i]
                            if val is None:
                                val = "NULL"
                            else:
                                val = str(val)
                            row_dict[col_name] = val
                        normalized_data.append(row_dict)
                    
                    # Sort the normalized data
                    normalized_data.sort(key=lambda x: [str(x.get(col, "")) for col in column_names])
                    
                    # Convert to JSON and calculate hash
                    data_json = json.dumps(normalized_data, sort_keys=True)
                    table_hash = hashlib.md5(data_json.encode()).hexdigest()
                    content_hashes.append(table_hash)
                except Exception as e:
                    logger.error(f"Error calculating hash for table {table}: {e}")
                    content_hashes.append("0")
            
            # Combine hashes
            content_hash = hashlib.md5(''.join(content_hashes).encode()).hexdigest()
            return content_hash
        except Exception as e:
            logger.error(f"Error calculating content hash: {e}")
            return "0"  # Fallback hash 