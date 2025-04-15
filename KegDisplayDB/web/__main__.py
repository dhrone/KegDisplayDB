"""
Entry point for KegDisplayDB web interface
"""

import sys
import os
import argparse
from .webinterface import start, parse_args

def main():
    # This block is executed when the module is run directly
    # e.g., python -m KegDisplayDB.web
    args = parse_args()
    return start(args)

if __name__ == "__main__":
    sys.exit(main()) 