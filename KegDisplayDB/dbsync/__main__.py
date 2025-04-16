"""
Entry point for KegDisplayDB database synchronization service
"""

import sys
import os
import argparse
from .service import DBSyncService, main as service_main

def main():
    """
    This block is executed when the module is run directly
    e.g., python -m KegDisplayDB.dbsync
    """
    # Use the main function from the service module
    return service_main()

if __name__ == "__main__":
    sys.exit(main()) 