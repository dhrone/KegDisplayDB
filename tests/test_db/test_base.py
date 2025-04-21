"""
Base test classes for database testing that ensure proper cleanup
"""

import unittest
import tempfile
import os
import logging
import shutil
import time
import glob

class DatabaseTest(unittest.TestCase):
    """Base test class for database tests with proper cleanup handling"""
    
    def setUp(self):
        """Set up a fresh database for each test."""
        # Create a temp directory with a unique prefix for easier debugging/cleanup
        self.temp_dir = tempfile.mkdtemp(prefix="kegdb_test_")
        self.db_path = os.path.join(self.temp_dir, 'test_db.db')
        
        # Store the original temp dir name for cleanup verification
        self._original_temp_dir = self.temp_dir
        
        # Override in subclasses to create your specific database manager
        self.db_manager = None
    
    def tearDown(self):
        """Clean up resources after each test."""
        try:
            # Ensure database connections are closed and garbage collected
            if hasattr(self, 'db_manager') and self.db_manager is not None:
                try:
                    # Explicitly set dbs to None to ensure it's garbage collected
                    if hasattr(self.db_manager, 'dbs') and self.db_manager.dbs is not None:
                        self.db_manager.dbs.shutdown(wait=True)
                        self.db_manager.dbs = None
                    # Set db_manager to None to help with garbage collection
                    self.db_manager = None
                except Exception as e:
                    logging.error(f"Error shutting down db_manager: {e}")
            
            # Add a small delay to ensure file handles are released
            time.sleep(0.1)
            
            # Clean up database files
            if hasattr(self, 'db_path') and os.path.exists(self.db_path):
                try:
                    os.remove(self.db_path)
                except Exception as e:
                    logging.error(f"Error removing DB file: {e}")
            
            # Clean up WAL and SHM files if they exist
            if hasattr(self, 'db_path'):
                for ext in ['-wal', '-shm']:
                    wal_file = self.db_path + ext
                    if os.path.exists(wal_file):
                        try:
                            os.remove(wal_file)
                        except Exception as e:
                            logging.error(f"Error removing {ext} file: {e}")
            
            # Find any remaining files in the temp directory
            if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
                try:
                    # Delete the temp directory with all its contents
                    shutil.rmtree(self.temp_dir, ignore_errors=True)
                except Exception as e:
                    logging.error(f"Error removing temp directory: {e}")
                    
                    # If shutil.rmtree fails, try a more aggressive approach
                    try:
                        # Get a list of all files in the directory
                        all_files = glob.glob(os.path.join(self.temp_dir, "*"))
                        for f in all_files:
                            try:
                                if os.path.isfile(f):
                                    os.remove(f)
                                elif os.path.isdir(f):
                                    shutil.rmtree(f, ignore_errors=True)
                            except Exception as e2:
                                logging.error(f"Could not remove {f}: {e2}")
                        
                        # Try to remove the directory again
                        if os.path.exists(self.temp_dir):
                            os.rmdir(self.temp_dir)
                    except Exception as e2:
                        logging.error(f"Failed final cleanup attempt: {e2}")
        
        except Exception as e:
            logging.error(f"Error in tearDown: {e}")
            
        finally:
            # Verify clean state for next test - if temp_dir still exists, log but don't fail
            if hasattr(self, '_original_temp_dir') and os.path.exists(self._original_temp_dir):
                logging.warning(f"WARNING: temp_dir {self._original_temp_dir} still exists after cleanup") 