#!/usr/bin/env python3
import os
import shutil
import sys

def clean_pycache():
    """Remove all __pycache__ directories and .pyc files"""
    for root, dirs, files in os.walk('.'):
        if '__pycache__' in dirs:
            shutil.rmtree(os.path.join(root, '__pycache__'))
        for file in files:
            if file.endswith('.pyc') or file.endswith('.pyo'):
                os.remove(os.path.join(root, file))

if __name__ == '__main__':
    print("Cleaning up __pycache__ directories and .pyc files...")
    clean_pycache()
    print("Done!")
    
    # Set environment variable to prevent future __pycache__ creation
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    print("\nTo prevent __pycache__ directories from being created in the future:")
    print("1. Add this line to your shell's rc file (e.g., ~/.zshrc):")
    print("   export PYTHONDONTWRITEBYTECODE=1")
    print("\n2. Or run your Python scripts with the -B flag:")
    print("   python -B your_script.py") 