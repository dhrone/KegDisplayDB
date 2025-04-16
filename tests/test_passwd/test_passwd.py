#!/usr/bin/env python3
import unittest
import os
import tempfile
import shutil
import bcrypt
from unittest.mock import patch
from KegDisplayDB.utils import passwd

class TestPasswdUtility(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        self.original_etc_dir = passwd.ETC_DIR
        self.original_passwd_path = passwd.PASSWD_PATH
        
        # Override the constants to use our test directory
        passwd.ETC_DIR = os.path.join(self.test_dir, "etc")
        passwd.PASSWD_PATH = os.path.join(passwd.ETC_DIR, "passwd")
        
        # Ensure the directory exists
        os.makedirs(passwd.ETC_DIR, exist_ok=True)
    
    def tearDown(self):
        # Restore original paths
        passwd.ETC_DIR = self.original_etc_dir
        passwd.PASSWD_PATH = self.original_passwd_path
        
        # Remove the temporary directory
        shutil.rmtree(self.test_dir)
    
    def test_add_user(self):
        # Test adding a user
        test_username = "testuser"
        test_password = "testpassword"
        
        result = passwd.add_user(test_username, test_password)
        self.assertTrue(result)
        
        # Verify user was added
        users = passwd.load_users()
        self.assertIn(test_username, users)
        
        # Verify password was hashed correctly
        password_hash = users[test_username]
        self.assertTrue(bcrypt.checkpw(test_password.encode('utf-8'), password_hash.encode('utf-8')))
    
    def test_delete_user(self):
        # First add a user
        test_username = "testuser"
        passwd.add_user(test_username, "testpassword")
        
        # Then delete the user
        result = passwd.delete_user(test_username)
        self.assertTrue(result)
        
        # Verify user was deleted
        users = passwd.load_users()
        self.assertNotIn(test_username, users)
    
    def test_invalid_username(self):
        # Test invalid username
        test_username = "test user"  # Contains space
        result = passwd.add_user(test_username, "testpassword")
        self.assertFalse(result)
        
        # Verify user was not added
        users = passwd.load_users()
        self.assertNotIn(test_username, users)
    
    @patch('getpass.getpass')
    def test_password_prompt(self, mock_getpass):
        # Mock getpass to return a test password
        mock_getpass.side_effect = ["testpassword", "testpassword"]
        
        # Test adding a user without providing a password
        test_username = "testuser"
        result = passwd.add_user(test_username)
        self.assertTrue(result)
        
        # Verify user was added
        users = passwd.load_users()
        self.assertIn(test_username, users)
    
    @patch('getpass.getpass')
    def test_password_mismatch(self, mock_getpass):
        # Mock getpass to return mismatched passwords, then matching ones
        mock_getpass.side_effect = ["testpassword", "wrongpassword", "testpassword", "testpassword"]
        
        # Test adding a user without providing a password
        test_username = "testuser"
        result = passwd.add_user(test_username)
        self.assertTrue(result)
        
        # Verify user was added
        users = passwd.load_users()
        self.assertIn(test_username, users)
    
    def test_list_users(self):
        # First add a couple of users
        passwd.add_user("user1", "password1")
        passwd.add_user("user2", "password2")
        
        # Capture stdout (can't easily test this without redirecting output)
        # For a real test, we would use a StringIO to capture output
        # Here we just make sure it doesn't raise any exceptions
        try:
            passwd.list_users()
            success = True
        except:
            success = False
        
        self.assertTrue(success)

if __name__ == '__main__':
    unittest.main() 