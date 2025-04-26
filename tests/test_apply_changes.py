from KegDisplayDB.db.database import DatabaseManager
import logging
import uuid
import json
import hashlib
import os
from datetime import datetime, UTC
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('KegDisplay')

# Create test database
db_path = 'tests/test_dbsync/test_apply_changes.db'
db = DatabaseManager(db_path)

# Function to create a test change
def create_test_change(table, operation, row_id, logical_clock, node_id=None):
    if node_id is None:
        node_id = str(uuid.uuid4())
    
    content = json.dumps({
        'idBeer': row_id,
        'Name': f'Test Beer {row_id}',
        'ABV': 5.0,
        'IBU': 20.0
    })
    content_hash = hashlib.md5(content.encode()).hexdigest()
    timestamp = datetime.now(UTC).isoformat()
    
    return [table, operation, row_id, timestamp, content, content_hash, logical_clock, node_id]

# Create test changes with different logical clocks and node IDs
node_1 = 'c0dbd44e-b08c-4b20-810e-7816d3771067'
node_2 = 'd99d6b31-8f9a-4465-aaa1-dceae2cdb24d'

# Group 1: Node 1, Clock 100, 5 changes
group1 = [create_test_change('beers', 'INSERT', i, 100, node_1) for i in range(1, 6)]

# Group 2: Node 2, Clock 200, 3 changes
group2 = [create_test_change('beers', 'INSERT', i+10, 200, node_2) for i in range(1, 4)]

# Group 3: Node 1, Clock 300, 2 changes
group3 = [create_test_change('beers', 'INSERT', i+20, 300, node_1) for i in range(1, 3)]

# Add one duplicate change with Node 2 but same content_hash as one from Group 1
dup_change = create_test_change('beers', 'INSERT', 3, 400, node_2)
dup_change[5] = group1[2][5]  # Same content hash as the third item in group1

# Combine all changes
all_changes = group1 + group2 + group3 + [dup_change]

# Apply the changes
logger.info(f'Applying {len(all_changes)} changes to test database')
db.apply_sync_changes(all_changes)

# Query the change_log to see the results
changes = db.execute('SELECT table_name, operation, row_id, logical_clock, node_id FROM change_log ORDER BY logical_clock')
logger.info(f'Resulting change_log entries: {len(changes)}')

print('\nChange log entries:')
for change in changes:
    print(f'Table: {change[0]}, Op: {change[1]}, Row: {change[2]}, Clock: {change[3]}, Node: {change[4]}')

# Check for duplicate detection
print('\nChecking for duplicates...')
# Apply the same changes again
db.apply_sync_changes(all_changes)

# Query the change_log to see if duplicates were added
changes_after = db.execute('SELECT table_name, operation, row_id, logical_clock, node_id FROM change_log ORDER BY logical_clock')
logger.info(f'Change_log entries after second apply: {len(changes_after)}')

print(f'Total entries after duplicate attempt: {len(changes_after)}')

# Close the database connection and clean up
try:
    # Close the database properly first
    logger.info('Closing database connection')
    # Force close the database service
    if hasattr(db, 'dbs'):
        db.dbs.shutdown(wait=True)
    
    # Short delay to ensure shutdown completes
    time.sleep(0.5)
    
    # Clean up WAL and SHM files
    wal_file = f"{db_path}-wal"
    shm_file = f"{db_path}-shm"
    
    for file_path in [wal_file, shm_file, db_path]:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f'Removed file: {file_path}')
            except OSError as e:
                logger.warning(f'Could not remove file {file_path}: {e}')
    
    logger.info('Cleanup completed')
except Exception as e:
    logger.error(f'Error during cleanup: {e}') 