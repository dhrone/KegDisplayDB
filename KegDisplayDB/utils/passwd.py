#!/usr/bin/env python3
import os
import sys
import argparse
import getpass
import bcrypt
import re
import logging

# Setup logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("passwd")

# Define paths
USER_HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(USER_HOME, ".KegDisplayDB")
ETC_DIR = os.path.join(CONFIG_DIR, "etc")
PASSWD_PATH = os.path.join(ETC_DIR, 'passwd')

def ensure_dirs_exist():
    """Ensure the required directories exist"""
    os.makedirs(ETC_DIR, exist_ok=True)

def load_users():
    """Load existing users from the passwd file"""
    users = {}
    try:
        with open(PASSWD_PATH, 'r') as f:
            for line in f:
                if line.strip():  # Skip empty lines
                    parts = line.strip().split(':')
                    if len(parts) >= 2:
                        username, password_hash = parts[0], parts[1]
                        users[username] = password_hash
    except FileNotFoundError:
        # If file doesn't exist, return empty dict
        pass
    return users

def save_users(users):
    """Save users to the passwd file"""
    with open(PASSWD_PATH, 'w') as f:
        for username, password_hash in users.items():
            f.write(f"{username}:{password_hash}\n")

def is_valid_username(username):
    """Check if username is valid (alphanumeric, _, -, .)"""
    return bool(re.match(r'^[a-zA-Z0-9_\-\.]+$', username))

def add_user(username, password=None):
    """Add a new user or update an existing user's password"""
    if not is_valid_username(username):
        logger.error("Invalid username. Use only letters, numbers, underscore, hyphen, or period.")
        return False
    
    # Load existing users
    users = load_users()
    
    # Check if user exists
    if username in users:
        logger.info(f"Updating password for user: {username}")
    else:
        logger.info(f"Adding new user: {username}")
    
    # If password not provided, prompt for it
    if password is None:
        while True:
            password = getpass.getpass("New password: ")
            if not password:
                logger.error("Password cannot be empty")
                continue
                
            confirm = getpass.getpass("Confirm password: ")
            if password != confirm:
                logger.error("Passwords do not match")
                continue
            break
    
    # Hash the password
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    # Save the user
    users[username] = password_hash
    save_users(users)
    
    logger.info(f"User '{username}' {'updated' if username in users else 'added'} successfully")
    return True

def delete_user(username):
    """Delete a user"""
    users = load_users()
    
    if username not in users:
        logger.error(f"User '{username}' does not exist")
        return False
    
    del users[username]
    save_users(users)
    
    logger.info(f"User '{username}' deleted successfully")
    return True

def list_users():
    """List all users"""
    users = load_users()
    
    if not users:
        logger.info("No users found")
        return
    
    logger.info("Users:")
    for username in sorted(users.keys()):
        logger.info(f"  {username}")

def main():
    """Main entry point for the passwd utility"""
    parser = argparse.ArgumentParser(description='KegDisplayDB web interface password management utility')
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Add user command
    add_parser = subparsers.add_parser('add', help='Add or update a user')
    add_parser.add_argument('username', help='Username to add or update')
    add_parser.add_argument('-p', '--password', help='Password (if not provided, will prompt)')
    
    # Delete user command
    delete_parser = subparsers.add_parser('delete', help='Delete a user')
    delete_parser.add_argument('username', help='Username to delete')
    
    # List users command
    subparsers.add_parser('list', help='List all users')
    
    # Parse args
    args = parser.parse_args()
    
    # Ensure directories exist
    ensure_dirs_exist()
    
    # Process commands
    if args.command == 'add':
        add_user(args.username, args.password)
    elif args.command == 'delete':
        delete_user(args.username)
    elif args.command == 'list':
        list_users()
    else:
        # If no command is provided, show help
        if len(sys.argv) == 1:
            # No arguments provided, show usage
            parser.print_help()
        elif len(sys.argv) == 2:
            # Only username provided, treat as add
            add_user(sys.argv[1])
        else:
            parser.print_help()

if __name__ == '__main__':
    main() 