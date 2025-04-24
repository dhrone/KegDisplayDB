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
import shutil
import traceback
import socket
import re
import concurrent.futures

from concurrent.futures import Future
from typing import Callable, TypeVar, Any, Optional


# Global transaction tracker dictionary to monitor all active database connections
active_connections = {}
active_connections_lock = threading.RLock()

# Database write serialization semaphore - ensures only one write operation happens at a time
db_write_semaphore = {}
db_write_semaphore_lock = threading.RLock()

logger = logging.getLogger("KegDisplay")

def format_stack_trace(stack):
    """Format a stack trace to a concise format: file:line(function)--file:line(function)--etc
    
    Args:
        stack: The stack trace to format (from traceback.extract_stack())
        
    Returns:
        str: Formatted stack trace
    """
    frames = []
    for frame in stack:
        # Extract just the filename without directory
        filename = os.path.basename(frame.filename)
        frames.append(f"{filename}:{frame.lineno}({frame.name})")
    
    # Join with double hyphen separators
    return "--".join(frames)

        
# Generic return type for transactional functions
R = TypeVar("R")

class DBService:
    def __init__(
        self,
        db_path: str,
        pragmas: Optional[dict[str, Any]] = None,
        busy_timeout: int = 5000
    ):
        """
        Initialize the DBService and open the SQLite connection.

        - db_path: Path to SQLite file.
        - pragmas: PRAGMA settings (e.g. {'journal_mode': 'WAL'}).
        - busy_timeout: Milliseconds to wait when DB is busy.
        """
        self._db_path = db_path
        self._pragmas = pragmas or {
            'journal_mode': 'WAL',
            'synchronous': 'NORMAL'
        }
        self._busy_timeout = busy_timeout

        # Shutdown control
        self._shutdown_event = threading.Event()
        self._shutdown_requested = False
        self._last_processed = time.monotonic()

        # Work queue: (kind, payload, future, enqueue_timestamp)
        self._queue: "queue.Queue[tuple[str, Any, Future, float]]" = queue.Queue()

        # Open SQLite connection
        try:
            self._conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                isolation_level=None
            )
            self._configure_pragmas()
        except Exception:
            logger.exception("DBS INIT ERROR: Failed to open SQLite connection.")
            raise

        # Start actor and watchdog threads
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        threading.Thread(target=self._watchdog, daemon=True).start()

    def _configure_pragmas(self) -> None:
        cursor = self._conn.cursor()
        for key, value in self._pragmas.items():
            cursor.execute(f"PRAGMA {key}={value};")
        cursor.execute(f"PRAGMA busy_timeout={self._busy_timeout};")
        cursor.close()

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        return ' '.join(sql.split())

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        if self._shutdown_event.is_set():
            raise RuntimeError("DBService is shut down")

        enqueue_time = time.monotonic()
        norm_sql = self._normalize_sql(sql)
        req_id = uuid.uuid4().hex
        qsize = self._queue.qsize()
        logger.debug(
            "DBS QUEUED EXECUTE | id=%s | qsize=%d | %s | %r",
            req_id, qsize, norm_sql, params
        )

        future = Future()
        # Payload for single: (sql, params, req_id)
        self._queue.put(("single", (sql, params, req_id), future, enqueue_time))

        try:
            return future.result(timeout=15)
        except TimeoutError:
            logger.error("DBS TIMEOUT EXECUTE | id=%s | %s", req_id, norm_sql)
            raise TimeoutError(f"Database operation timed out: {norm_sql}")

    def run_in_transaction(self, fn: Callable[[sqlite3.Connection], R]) -> R:
        if self._shutdown_event.is_set():
            raise RuntimeError("DBService is shut down")

        if fn is None or not callable(fn):
            msg = f"Invalid transaction function: {fn}"
            logger.error("DBS ERROR: %s", msg)
            raise TypeError(msg)

        enqueue_time = time.monotonic()
        fn_name = getattr(fn, '__name__', repr(fn))
        tx_id = uuid.uuid4().hex
        qsize = self._queue.qsize()
        logger.debug(
            "DBS QUEUED TRANSACTION | id=%s | qsize=%d | %s",
            tx_id, qsize, fn_name
        )

        future: Future = Future()
        # Payload for tx: (fn, tx_id)
        self._queue.put(("tx", (fn, tx_id), future, enqueue_time))

        try:
            return future.result(timeout=20)
        except TimeoutError:
            logger.error("DBS TIMEOUT TRANSACTION | id=%s | %s", tx_id, fn_name)
            raise TimeoutError(f"Database transaction timed out: {fn_name}")

    def shutdown(self, wait: bool = True) -> None:
        if self._shutdown_requested:
            logger.debug("DBS SHUTDOWN already requested, skipping")
            return
        self._shutdown_requested = True

        logger.debug("DBS SHUTDOWN requested, enqueuing shutdown message")
        future = Future()
        self._queue.put(("shutdown", None, future, time.monotonic()))

        if wait:
            try:
                future.result(timeout=5)
            except Exception as e:
                logger.warning("DBS SHUTDOWN WAIT ERROR: %s", e)

        logger.debug("DBS SHUTDOWN complete")

    def _run(self) -> None:
        while True:
            try:
                kind, payload, future, enqueue_time = self._queue.get()
            except Exception:
                continue

            dequeue_time = time.monotonic()
            wait_time = dequeue_time - enqueue_time

            if kind == "shutdown":
                logger.debug("DBS PROCESS SHUTDOWN | waited=%.3fs", wait_time)
                try:
                    self._conn.close()
                except Exception:
                    logger.exception("DBS ERROR closing connection")
                future.set_result(None)
                break

            if kind == "single":
                sql, params, req_id = payload
                norm_sql = self._normalize_sql(sql)
                logger.debug(
                    "DBS PROCESS EXECUTE | id=%s | waited=%.3fs | %s | %r",
                    req_id, wait_time, norm_sql, params
                )
                start = time.monotonic()
                cur = self._conn.execute(sql, params)
                sql_up = norm_sql.upper()
                if sql_up.startswith(("SELECT", "PRAGMA")):
                    result = cur.fetchall()
                elif sql_up.startswith("INSERT"):
                    result = cur.lastrowid
                    self._conn.commit()
                else:
                    result = cur.rowcount
                    self._conn.commit()
                duration = time.monotonic() - start
                log_fn = logger.warning if duration > 0.5 else logger.debug
                log_fn("DBS COMPLETE EXECUTE | id=%s | duration=%.3fs", req_id, duration)
                future.set_result(result)

            elif kind == "tx":
                fn, tx_id = payload
                fn_name = getattr(fn, '__name__', repr(fn))
                logger.debug(
                    "DBS PROCESS TRANSACTION | id=%s | %s | waited=%.3fs",
                    tx_id, fn_name, wait_time
                )
                start = time.monotonic()
                try:
                    self._conn.execute("BEGIN;")
                    outcome = fn(self._conn)
                    self._conn.commit()
                    duration = time.monotonic() - start
                    log_fn = logger.warning if duration > 0.5 else logger.debug
                    log_fn(
                        "DBS COMPLETE TRANSACTION | id=%s | %s | duration=%.3fs",
                        tx_id, fn_name, duration
                    )
                    future.set_result(outcome)
                except Exception as e:
                    self._conn.rollback()
                    logger.exception(
                        "DBS TRANSACTION FAILED | id=%s | %s | error=%s",
                        tx_id, fn_name, e
                    )
                    future.set_exception(e)

            else:
                future.set_exception(ValueError(f"DBS UNKNOWN REQUEST KIND: {kind}"))

            self._queue.task_done()
            self._last_processed = time.monotonic()

        logger.debug("DBS actor loop exiting")
        self._shutdown_event.set()

    def _watchdog(self):
        while not self._shutdown_event.is_set():
            time.sleep(5)
            if self._queue.qsize() == 0:
                self._last_processed = time.monotonic() # reset last processed time if queue is empty            
            delta = time.monotonic() - self._last_processed
            if delta > 2.0:
                logger.error("DBS WATCHDOG: no requests processed in %.1fs", delta)



class DatabaseManager:
    """
    Handles core database operations for the KegDisplay system.
    Manages the beer and tap tables and provides CRUD operations.
    """
    
    # Class variables to store connection pools for different database paths
    _rw_connection_pools = {}
    _ro_connection_pools = {}
    _pool_locks = {}
    
    def __init__(self, db_path, pool_size=5):
        """Initialize the database manager
        
        Args:
            db_path: Path to the SQLite database file
            pool_size: Size of the connection pool
        """
        self.db_path = db_path        
        self.dbs = DBService(db_path)
        self.initialize_tables()

    
    def __del__(self):
        """Cleanup method to ensure DBService is properly shut down"""
        if hasattr(self, 'dbs') and self.dbs is not None:
            try:
                logger.debug(f"DatabaseManager.__del__ shutting down DBService for {self.db_path}")
                self.dbs.shutdown(wait=False)  # Use non-blocking shutdown in __del__
            except Exception as e:
                logger.error(f"Error shutting down DBService in __del__: {e}")
            finally:
                # Remove the reference to the DBService to allow garbage collection
                self.dbs = None
    
    def execute(self, sql, params=()):
        """
        Execute a single SQL statement through DBService
        
        Args:
            sql: SQL statement to execute
            params: Parameters for the SQL statement
            conn: Optional connection for backwards compatibility (ignored)
            
        Returns:
            For SELECT statements: List of rows
            For other DML: Number of affected rows
        """
            
        return self.dbs.execute(sql, params)
    
    def transaction(self, fn=None):
        """
        Run a function inside a transaction through DBService
        
        Args:
            fn: Function to run in the transaction
            
        Returns:
            Result of the function or transaction context manager if fn is None
        """

        return self.dbs.run_in_transaction(fn)
    
    def initialize_tables(self):
        """Initialize database tables if they don't exist
        
        Args:
            conn: Optional database connection to use (to avoid nested transactions)
        """
        try:
            # Create beers table if it doesn't exist
            self.execute('''
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
            self.execute('''
                CREATE TABLE IF NOT EXISTS taps (
                    idTap INTEGER PRIMARY KEY,
                    idBeer INTEGER
                )
            ''')
            
            # Create change_log table if it doesn't exist
            self.execute('''
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
            self.execute('''
                CREATE TABLE IF NOT EXISTS version (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    logical_clock INTEGER DEFAULT 0,
                    node_id TEXT
                )
            ''')
            
            # Initialize version table with a valid record if it doesn't exist
            count_result = self.execute("SELECT COUNT(*) FROM version WHERE id = 1")
            if count_result and count_result[0][0] == 0:
                # Generate a node ID for this instance
                node_id = str(uuid.uuid4())
                
                # Calculate initial hash for empty tables
                tables = ['beers', 'taps']
                initial_hash = self._calculate_db_hash(tables)
                
                # Create initial version record
                timestamp = datetime.now(UTC).isoformat()
                self.execute(
                    "INSERT INTO version (timestamp, hash, logical_clock, node_id) VALUES (?, ?, 0, ?)",
                    (timestamp, initial_hash, node_id)
                )
            
            logger.info("Database tables initialized")
        except Exception as e:
            logger.error(f"Error initializing database tables: {e}")
            raise
    
    
    # ---- Beer Management Methods ----
    
    def add_beer(self, name, abv=None, ibu=None, color=None, og=None, fg=None, 
                description=None, brewed=None, kegged=None, tapped=None, notes=None):
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
        
        # Use the query method which now handles transactions
        sql = '''
            INSERT INTO beers (
                Name, ABV, IBU, Color, OriginalGravity, FinalGravity,
                Description, Brewed, Kegged, Tapped, Notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = (name, abv, ibu, color, og, fg, description, brewed, kegged, tapped, notes)
        
        try:
            beer_id = self.execute(sql, params)
            logger.info(f"Added beer '{name}' with ID {beer_id}")
            return beer_id
        except Exception as e:
            logger.error(f"Error adding beer '{name}': {e}")
            raise
    
    def update_beer(self, beer_id, name=None, abv=None, ibu=None, color=None, og=None, fg=None,
                   description=None, brewed=None, kegged=None, tapped=None, notes=None):
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
        
        try:
            def update_transaction(conn):
                # Get existing data for any fields not specified
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
                existing_beer = cursor.fetchone()
                
                if not existing_beer:
                    logger.error(f"Cannot update beer with ID {beer_id}: Beer not found")
                    return False
                
                # Get column names from cursor description
                columns = [desc[0] for desc in cursor.description]
                existing_beer_dict = {columns[i]: existing_beer[i] for i in range(len(columns))}
                
                # Update only the fields that were specified
                update_name = name if name is not None else existing_beer_dict['Name']
                update_abv = abv if abv is not None else existing_beer_dict['ABV']
                update_ibu = ibu if ibu is not None else existing_beer_dict['IBU']
                update_color = color if color is not None else existing_beer_dict['Color']
                update_og = og if og is not None else existing_beer_dict['OriginalGravity']
                update_fg = fg if fg is not None else existing_beer_dict['FinalGravity']
                update_description = description if description is not None else existing_beer_dict['Description']
                update_brewed = brewed if brewed is not None else existing_beer_dict['Brewed']
                update_kegged = kegged if kegged is not None else existing_beer_dict['Kegged']
                update_tapped = tapped if tapped is not None else existing_beer_dict['Tapped']
                update_notes = notes if notes is not None else existing_beer_dict['Notes']
                
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
                
                return True
            
            # Execute the transaction
            result = self.dbs.run_in_transaction(update_transaction)
            
            if result:
                logger.info(f"Updated beer {beer_id}")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error updating beer with ID {beer_id}: {e}")
            raise
    
    def delete_beer(self, beer_id):
        """Delete a beer from the database
        
        Args:
            beer_id: ID of the beer to delete
            
        Returns:
            success: True if the beer was deleted, False if not found
        """
        try:
            # First check if beer exists
            beer = self.execute("SELECT Name FROM beers WHERE idBeer = ?", (beer_id,))
            
            if not beer:
                logger.warning(f"Beer with ID {beer_id} not found for deletion")
                return False
            
            beer_name = beer[0][0]
            
            # Delete the beer
            result = self.execute("DELETE FROM beers WHERE idBeer = ?", (beer_id,))
            
            if result > 0:
                logger.info(f"Deleted beer '{beer_name}' with ID {beer_id}")
                return True
            else:
                logger.warning(f"Failed to delete beer with ID {beer_id}")
                return False
        except Exception as e:
            logger.error(f"Error deleting beer with ID {beer_id}: {e}")
            raise
    
    def get_beer(self, beer_id):
        """Get a beer by ID
        
        Args:
            beer_id: ID of the beer to retrieve
            conn: Optional database connection to use
            
        Returns:
            beer: Dictionary with beer information or None if not found
        """
        try:
            results = self.execute("SELECT * FROM beers WHERE idBeer = ?", (beer_id,))
            
            if not results:
                return None
                
            # Convert to dictionary using column names
            columns = [
                "idBeer", "Name", "ABV", "IBU", "Color", "OriginalGravity", 
                "FinalGravity", "Description", "Brewed", "Kegged", "Tapped", "Notes"
            ]
            beer_dict = {columns[i]: results[0][i] for i in range(len(columns))}
            return beer_dict
        except Exception as e:
            logger.error(f"Error retrieving beer with ID {beer_id}: {e}")
            return None
    
    def get_all_beers(self):
        """Get all beers from the database
        
        Args:
            conn: Optional database connection to use
            
        Returns:
            beers: List of dictionaries with beer information
        """
        try:
            results = self.execute("SELECT * FROM beers ORDER BY idBeer")
            
            if not results:
                return []
                
            # Convert rows to dictionaries using column names
            columns = [
                "idBeer", "Name", "ABV", "IBU", "Color", "OriginalGravity", 
                "FinalGravity", "Description", "Brewed", "Kegged", "Tapped", "Notes"
            ]
            
            beer_dicts = []
            for row in results:
                beer_dict = {columns[i]: row[i] for i in range(len(columns))}
                beer_dicts.append(beer_dict)
                
            return beer_dicts
        except Exception as e:
            logger.error(f"Error retrieving all beers: {e}")
            return []
    
    # ---- Tap Management Methods ----
    
    def add_tap(self, tap_id=None, beer_id=None):
        """Add a new tap to the database
        
        Args:
            tap_id: ID for the tap (optional, auto-generated if not provided)
            beer_id: ID of the beer to assign (optional)
            conn: Optional database connection to use
            
        Returns:
            id: ID of the newly added tap
        """
        try:
            if tap_id:
                # Check if tap with this ID already exists
                existing_tap = self.execute("SELECT COUNT(*) FROM taps WHERE idTap = ?", (tap_id,))
                
                if existing_tap and existing_tap[0][0] > 0:
                    logger.warning(f"Tap with ID {tap_id} already exists")
                    return None
                
                # Insert with specified ID
                self.execute("INSERT INTO taps (idTap, idBeer) VALUES (?, ?)", (tap_id, beer_id))
                return tap_id
            else:
                # Auto-generate ID
                last_id = self.execute("INSERT INTO taps (idBeer) VALUES (?)", (beer_id,))
                logger.info(f"Added tap with beer ID {beer_id}")
                return last_id
        except Exception as e:
            logger.error(f"Error adding tap: {e}")
            raise
    
    def update_tap(self, tap_id, beer_id):
        """Update a tap's beer assignment
        
        Args:
            tap_id: ID of the tap to update
            beer_id: ID of the beer to assign (None to unassign)
            conn: Optional database connection to use
            
        Returns:
            success: True if the tap was updated, False if not found
        """
        try:
            # Check if tap exists
            tap_exists = self.execute("SELECT idTap FROM taps WHERE idTap = ?", (tap_id,))
            
            if not tap_exists:
                logger.warning(f"Tap with ID {tap_id} not found for update")
                return False
            
            # Update the tap
            self.execute("UPDATE taps SET idBeer = ? WHERE idTap = ?", (beer_id, tap_id))
            
            logger.info(f"Updated tap {tap_id} with beer ID {beer_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating tap {tap_id}: {e}")
            raise
    
    def delete_tap(self, tap_id):
        """Delete a tap from the database
        
        Args:
            tap_id: ID of the tap to delete
            conn: Optional database connection to use
            
        Returns:
            success: True if deleted, False if not found
        """
        try:
            # Check if tap exists
            tap_exists = self.execute("SELECT idTap FROM taps WHERE idTap = ?", (tap_id,))
            
            if not tap_exists:
                logger.warning(f"Tap with ID {tap_id} not found for deletion")
                return False
            
            # Delete the tap
            result = self.execute("DELETE FROM taps WHERE idTap = ?", (tap_id,))
            
            logger.info(f"Deleted tap {tap_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting tap {tap_id}: {e}")
            raise
    
    def get_tap(self, tap_id):
        """Get a tap by ID
        
        Args:
            tap_id: ID of the tap to retrieve
            conn: Optional database connection to use
            
        Returns:
            tap: Dictionary with tap information or None if not found
        """
        try:
            results = self.execute(
                "SELECT t.*, b.Name as BeerName FROM taps t "
                "LEFT JOIN beers b ON t.idBeer = b.idBeer "
                "WHERE t.idTap = ?", 
                (tap_id,)
            )
            
            if not results:
                return None
                
            # Get column names (need to determine them dynamically due to join)
            columns = ["idTap", "idBeer", "BeerName"]
            tap_dict = {columns[i]: results[0][i] for i in range(len(results[0]))}
            return tap_dict
        except Exception as e:
            logger.error(f"Error retrieving tap {tap_id}: {e}")
            return None
    
    def get_all_taps(self):
        """Get all taps with their beer information
        
        Args:
            conn: Optional database connection to use
            
        Returns:
            taps: List of dictionaries with tap information
        """
        try:
            results = self.execute(
                "SELECT t.*, b.Name as BeerName FROM taps t "
                "LEFT JOIN beers b ON t.idBeer = b.idBeer "
                "ORDER BY t.idTap"
            )
            
            if not results:
                return []
                
            # Get column names (need to determine them dynamically due to join)
            columns = ["idTap", "idBeer", "BeerName"]
            
            tap_dicts = []
            for row in results:
                tap_dict = {columns[i]: row[i] for i in range(len(row))}
                tap_dicts.append(tap_dict)
                
            return tap_dicts
        except Exception as e:
            logger.error(f"Error retrieving all taps: {e}")
            return []
    
    def get_tap_with_beer(self, beer_id):
        """Get IDs of taps that have a specific beer assigned
        
        Args:
            beer_id: ID of the beer to look for
            conn: Optional database connection to use
            
        Returns:
            list: List of tap IDs that have the beer assigned
        """
        try:
            rows = self.execute(
                "SELECT idTap FROM taps WHERE idBeer = ?",
                (beer_id,)
            )
            
            return [row[0] for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Error retrieving taps with beer {beer_id}: {e}")
            return []

    def clear_change_log(self):
        """Delete all records from the change_log table
        
        Returns:
            success: True if the operation was successful
        """
        try:
            self.execute("DELETE FROM change_log")
            logger.debug(f"Cleared all records from change_log table")
            return True
        except Exception as e:
            logger.error(f"Error clearing change_log table: {e}")
            raise 

    def clear_beer(self):
        """Delete all records from the beers table
        
        Returns:
            success: True if the operation was successful
        """
        try:
            self.execute("DELETE FROM beers")
            logger.debug(f"Cleared all records from beers table")
            return True
        except Exception as e:
            logger.error(f"Error clearing beers table: {e}")
            raise
    
    def clear_tap(self):
        """Delete all records from the taps table
        
        Returns:
            success: True if the operation was successful
        """
        try:
            self.execute("DELETE FROM taps")
            logger.debug(f"Cleared all records from taps table")
            return True
        except Exception as e:
            logger.error(f"Error clearing taps table: {e}")
            raise
            
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
        total_applied_changes = 0
        total_failed_changes = 0
        highest_logical_clock = 0
        
        try:
            # Get current logical clock
            current_clock_row = self.execute(
                "SELECT logical_clock, node_id FROM version WHERE id = 1"
            )
            current_clock = current_clock_row[0][0] if current_clock_row and current_clock_row[0][0] is not None else 0
            local_node_id = current_clock_row[0][1] if current_clock_row and current_clock_row[0][1] is not None else str(uuid.uuid4())
            
            # Sort the changes by logical clock and origin node
            changes = sorted(
                changes,
                key=lambda c: (
                    c[6] if len(c) > 6 else 0,          # logical clock
                    c[7] if len(c) > 7 else local_node_id  # origin node
                )
            )
            
            # Process changes in batches of 100
            batch_size = 100
            
            # Process all changes in batches
            for i in range(0, len(changes), batch_size):
                batch = changes[i:i+batch_size]
                batch_clock = current_clock  # Current clock at the start of this batch
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(changes)-1)//batch_size + 1} ({len(batch)} changes)")
                
                # Define the transaction function for this batch
                def process_batch(conn):
                    nonlocal batch_clock, total_applied_changes, total_failed_changes, highest_logical_clock
                    
                    applied_changes = 0
                    failed_changes = 0
                    batch_highest_clock = batch_clock

                    def do_update_version(conn, timestamp, hash, logical_clock, node_id):
                        # Update version table with new logical clock and timestamp
                        self.execute(
                            """
                            BEGIN TRANSACTION;
                            UPDATE version SET timestamp = ?, hash = ?, logical_clock = ?, node_id = ?
                            WHERE id = 1;
                            COMMIT;
                            """,
                            (timestamp, hash, logical_clock, node_id)
                        )
                    
                    # Process each change in the batch
                    for change_index, change in enumerate(batch):
                        try:
                            # Ensure the change has all the required fields
                            if len(change) != 8:
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
                            logical_clock = change[6]
                            node_id = change[7]

                            error_message = f"Error applying {operation} change to {table_name}.{row_id}"
                            
                            # Check if this change is already in our change_log
                            cursor = conn.execute(
                                """
                                SELECT COUNT(*) FROM change_log 
                                WHERE table_name = ? AND operation = ? AND row_id = ? 
                                  AND ((logical_clock = ? AND node_id = ?) OR 
                                       (logical_clock > ? AND content_hash = ?))
                                """,
                                (table_name, operation, row_id, logical_clock, node_id, 
                                 logical_clock, content_hash)
                            )
                            existing_change = cursor.fetchone()
                            
                            if existing_change and existing_change[0] > 0:
                                # Skip changes we've already processed
                                logger.debug(f"Skipping duplicate change: {operation} on {table_name}.{row_id} with clock {logical_clock} from node {node_id}")
                                # Count as successful to track statistics properly
                                applied_changes += 1
                                continue
                                
                            # Verify content hash (security check)
                            if hashlib.md5(content.encode()).hexdigest() != content_hash:
                                logger.warning(f"Content hash mismatch for change at index {change_index}")
                                failed_changes += 1
                                continue
                            
                            # Update our logical clock (Lamport rule: localClock = max(localClock, t) + 1)
                            new_clock = max(batch_clock, logical_clock) + 1
                            batch_clock = new_clock  # Update for next iteration
                            
                            # Track the highest computed clock
                            if new_clock > batch_highest_clock:
                                batch_highest_clock = new_clock
                            
                            logger.debug(f"Applying change: {operation} to {table_name}.{row_id} (logical clock: {logical_clock}, our new clock: {new_clock})")
                            
                            # Apply the change based on operation type
                            if operation in ['INSERT', 'UPDATE', 'DELETE', 'CLEAR']:
                                try:
                                    # Parse the content as JSON and build the SQL
                                    row_data = json.loads(content)

                                    conn.execute('BEGIN TRANSACTION;')
                                    
                                    if operation == 'INSERT':
                                        # Build INSERT statement
                                        columns = ', '.join(row_data.keys())
                                        placeholders = ', '.join(['?'] * len(row_data))
                                        sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders});"
                                        conn.execute(sql, list(row_data.values()))
                                        
                                    elif operation == 'UPDATE':
                                        # Build UPDATE statement
                                        set_clause = ', '.join([f"{col} = ?" for col in row_data.keys()])
                                        sql = f"UPDATE {table_name} SET {set_clause} WHERE rowid = ?;"
                                        params = list(row_data.values()) + [row_id]
                                        conn.execute(sql, params)

                                    elif operation == 'DELETE':
                                        # Build DELETE statement
                                        sql = f"DELETE FROM {table_name} WHERE rowid = ?;"
                                        conn.execute(sql, (row_id,))

                                    elif operation == 'CLEAR':
                                        # Build CLEAR statement
                                        error_message = f"Error applying CLEAR to database"
                                        sql = f"DELETE FROM beers; UPDATE taps SET idBeer = NULL;"
                                        conn.execute(sql)
                                    
                                    # Log the change in our change_log table with OUR new clock value
                                    # but preserve the ORIGINAL node_id to maintain provenance
                                    logger.debug(f"Preserving original node_id {node_id} when recording change in local log")
                                    conn.execute(
                                        """
                                        INSERT INTO change_log 
                                        (table_name, operation, row_id, timestamp, content, content_hash, logical_clock, node_id) 
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (table_name, operation, row_id, timestamp, content, content_hash, new_clock, node_id)
                                    )

                                    conn.execute('COMMIT;')

                                    applied_changes += 1
                                    
                                except Exception as e:
                                    logger.error(f"{error_message}: {e}")
                                    failed_changes += 1
                                    
                            else:
                                logger.warning(f"Unknown operation '{operation}' in change at index {change_index}")
                                failed_changes += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing change at index {change_index}: {e}")
                            failed_changes += 1
                    
                    # Update tracking variables after batch processing
                    total_applied_changes += applied_changes
                    total_failed_changes += failed_changes
                    if batch_highest_clock > highest_logical_clock:
                        highest_logical_clock = batch_highest_clock
                    
                    # Return the new clock value to use for the next batch
                    return batch_highest_clock
                
                # Execute the batch transaction
                batch_result = self.dbs.run_in_transaction(process_batch)
                current_clock = batch_result  # Update current clock for next batch
            
            # Update the version table with the highest computed logical clock
            if highest_logical_clock > 0:
                # Calculate current database hash
                tables = ['beers', 'taps']
                new_hash = self._calculate_db_hash(tables)
                
                # Update version table with new logical clock and timestamp
                timestamp = datetime.now(UTC).isoformat()
                
                self.execute(
                    """
                    UPDATE version 
                    SET timestamp = ?,
                        hash = ?,
                        logical_clock = ?,
                        node_id = ?
                    WHERE id = 1
                    """,
                    (timestamp, new_hash, highest_logical_clock, local_node_id)
                )
                
                logger.info(f"Updated version with logical clock {highest_logical_clock}")
                
            logger.info(f"Successfully applied {total_applied_changes} changes, {total_failed_changes} failed")
            return total_applied_changes > 0
                
        except Exception as e:
            logger.error(f"Error applying sync changes: {e}")
            raise
    
    def import_from_file(self, temp_db_path):
        """Import the entire database from a file using ATTACH DATABASE
        
        Args:
            temp_db_path: Path to the database file to import from
            
        Returns:
            success: Whether the import was successful
        """
        logger.info(f"Importing database from {temp_db_path}")
        
        # Ensure the temp database file exists
        if not os.path.exists(temp_db_path):
            logger.error(f"Import failed: Source database file {temp_db_path} does not exist")
            return False
        
        try:
            # Create a backup before importing if the database exists
            if os.path.exists(self.db_path):
                backup_created = self._create_backup_before_import()
                if backup_created:
                    logger.info("Created backup before importing database")
                else:
                    logger.warning("Failed to create backup before importing database")
            
            # Get absolute paths for both databases to ensure consistent referencing
            abs_temp_path = os.path.abspath(temp_db_path)
            
            # Define the import function that will run in a transaction
            def perform_import(conn):
                try:
                    # Attach the temporary database
                    conn.execute(f"ATTACH DATABASE '{abs_temp_path}' AS temp_db")
                    
                    # Check if required tables exist in the temp database
                    cursor = conn.execute("SELECT name FROM temp_db.sqlite_master WHERE type='table' AND (name='beers' OR name='taps')")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    if 'beers' not in tables or 'taps' not in tables:
                        logger.error("Import failed: Required tables not found in source database")
                        conn.execute("DETACH DATABASE temp_db")
                        return False
                    
                    # Get our local node_id
                    cursor = conn.execute("SELECT node_id FROM version WHERE id = 1")
                    local_node_id_row = cursor.fetchone()
                    local_node_id = local_node_id_row[0] if local_node_id_row else str(uuid.uuid4())
                    
                    # Clear existing tables
                    conn.execute("DELETE FROM beers")
                    conn.execute("DELETE FROM taps")
                    
                    # Get column names from both tables to ensure schema compatibility
                    cursor = conn.execute("PRAGMA table_info(beers)")
                    local_beer_columns = [row[1] for row in cursor.fetchall()]
                    
                    cursor = conn.execute("PRAGMA temp_db.table_info(beers)")
                    temp_beer_columns = [row[1] for row in cursor.fetchall()]
                    
                    # Find common columns for beers
                    common_beer_columns = [col for col in temp_beer_columns if col in local_beer_columns]
                    beer_columns_str = ", ".join(common_beer_columns)
                    
                    # Copy beers data
                    conn.execute(f"INSERT INTO beers ({beer_columns_str}) SELECT {beer_columns_str} FROM temp_db.beers")
                    
                    # Do the same for taps
                    cursor = conn.execute("PRAGMA table_info(taps)")
                    local_tap_columns = [row[1] for row in cursor.fetchall()]
                    
                    cursor = conn.execute("PRAGMA temp_db.table_info(taps)")
                    temp_tap_columns = [row[1] for row in cursor.fetchall()]
                    
                    # Find common columns for taps
                    common_tap_columns = [col for col in temp_tap_columns if col in local_tap_columns]
                    tap_columns_str = ", ".join(common_tap_columns)
                    
                    # Copy taps data
                    conn.execute(f"INSERT INTO taps ({tap_columns_str}) SELECT {tap_columns_str} FROM temp_db.taps")
                    
                    # Handle version information
                    try:
                        # Check if version table exists in the temp database
                        cursor = conn.execute("SELECT COUNT(*) FROM temp_db.sqlite_master WHERE type='table' AND name='version'")
                        if cursor.fetchone()[0] > 0:
                            # Check if logical_clock column exists
                            cursor = conn.execute("PRAGMA temp_db.table_info(version)")
                            version_columns = [row[1] for row in cursor.fetchall()]
                            
                            if 'logical_clock' in version_columns:
                                # Get version info from temp db
                                cursor = conn.execute("SELECT timestamp, hash, logical_clock, node_id FROM temp_db.version WHERE id = 1")
                                version_row = cursor.fetchone()
                                
                                if version_row:
                                    timestamp, db_hash, logical_clock, node_id = version_row
                                    logger.info(f"Importing version data with logical_clock {logical_clock}")
                                    
                                    # Get our current logical clock
                                    cursor = conn.execute("SELECT logical_clock FROM version WHERE id = 1")
                                    local_clock = cursor.fetchone()[0] or 0
                                    
                                    # Apply Lamport rule: new_clock = max(local_clock, imported_clock) + 1  
                                    new_clock = max(local_clock, logical_clock) + 1
                                    
                                    # Update with new values
                                    conn.execute("""
                                        UPDATE version 
                                        SET timestamp = ?, hash = ?, logical_clock = ?
                                        WHERE id = 1
                                    """, (datetime.now(UTC).isoformat(), self._calculate_db_hash(['beers', 'taps']), new_clock))
                    except Exception as e:
                        logger.warning(f"Could not import version information: {e}")
                    
                    # Detach the temporary database
                    conn.execute("DETACH DATABASE temp_db")
                    
                    # Count imported records
                    cursor = conn.execute("SELECT COUNT(*) FROM beers")
                    beer_count = cursor.fetchone()[0]
                    
                    cursor = conn.execute("SELECT COUNT(*) FROM taps")
                    tap_count = cursor.fetchone()[0]
                    
                    logger.info(f"Successfully imported database with {beer_count} beers and {tap_count} taps")
                    return True
                    
                except Exception as e:
                    # Make sure to detach the database even if an error occurs
                    try:
                        conn.execute("DETACH DATABASE IF EXISTS temp_db")
                    except:
                        pass
                    logger.error(f"Error during database import transaction: {e}")
                    return False
            
            # Execute the import using DBService transaction
            result = self.dbs.run_in_transaction(perform_import)
            return result
                
        except Exception as e:
            logger.error(f"Error importing database: {e}")
            return False
    
    def _create_backup_before_import(self, error_backup=False):
        """Create a backup before importing a database
        
        Args:
            error_backup: Whether to create an error backup
            
        Returns:
            success: Whether the backup was successful
        """
        try:
            # Check if database exists
            if not os.path.exists(self.db_path):
                logger.info("No database to backup before import")
                return True
            
            # Get the directory and base name for the database
            db_dir = os.path.dirname(self.db_path)
            db_name = os.path.basename(self.db_path)

            backup_name = f"{db_name}.{i}.err.bak" if error_backup else f"{db_name}.{i}.bak"
            
            # Use a simple rotating backup scheme (max 5 backups)
            max_backups = 5
            
            # Find an available backup slot (1-5)
            for i in range(1, max_backups + 1):
                backup_path = os.path.join(db_dir, backup_name)
                if not os.path.exists(backup_path):
                    break
            else:
                # If all slots are taken, use the oldest backup
                backup_files = []
                for i in range(1, max_backups + 1):
                    path = os.path.join(db_dir, backup_name)
                    if os.path.exists(path):
                        backup_files.append((path, os.path.getmtime(path)))
                
                # Sort by modification time (oldest first)
                backup_files.sort(key=lambda x: x[1])
                if backup_files:
                    backup_path = backup_files[0][0]
                else:
                    i = 1
                    backup_path = os.path.join(db_dir, backup_name)
            
            # Create the backup
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"Created pre-import backup at {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to create pre-import backup: {e}")
            return False
    


    def _calculate_db_hash(self, tables):
        """Calculate a hash based on database content for version tracking
        
        Args:
            tables: List of table names to include in the hash
            
        Returns:
            str: MD5 hash of relevant database content
        """
        try:
            # Calculate content-based hash from all tracked tables
            content_hashes = []
            for table in tables:
                # Get all rows from the table for hashing
                try:
                    rows = self.execute(f"SELECT * FROM {table}")
                    
                    # Debug breakpoint to inspect rows type
                    # import pdb; pdb.set_trace()  # Uncomment this line to enable breakpoint
                    
                    # Handle case when execute returns an integer instead of rows
                    if isinstance(rows, int):
                        logger.warning(f"No rows returned for table {table} during hash calculation")
                        rows = []
                    
                    # Get column names
                    column_info = self.execute(f"PRAGMA table_info({table})")
                    # Handle case when execute returns an integer instead of column info
                    if isinstance(column_info, int):
                        logger.warning(f"No column info returned for table {table} during hash calculation")
                        column_info = []
                        
                    column_names = [col[1] for col in column_info] if column_info else []
                    
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
        
