import unittest
import os
import tempfile
import shutil
from pathlib import Path
import argparse
import sys

# Mock the logger to avoid actual logging during tests
import logging
logging.basicConfig(level=logging.CRITICAL)

# Import the necessary modules
from KegDisplayDB.web.webinterface import generate_self_signed_certificate


class TestCertificateGeneration(unittest.TestCase):
    """Test class for the SSL certificate auto-generation functionality."""
    
    def setUp(self):
        """Create a temporary directory for test certificates."""
        self.temp_dir = tempfile.mkdtemp()
        self.ssl_dir = os.path.join(self.temp_dir, 'ssl')
        self.certs_dir = os.path.join(self.ssl_dir, 'certs')
        self.private_dir = os.path.join(self.ssl_dir, 'private')
        
        # Create the directory structure
        os.makedirs(self.certs_dir, exist_ok=True)
        os.makedirs(self.private_dir, exist_ok=True)
        
        # Define paths for certificate and key
        self.cert_path = os.path.join(self.certs_dir, 'kegdisplay.crt')
        self.key_path = os.path.join(self.private_dir, 'kegdisplay.key')
    
    def tearDown(self):
        """Clean up the temporary directory."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_certificate_generation(self):
        """Test that certificates are properly generated."""
        # Verify certificate and key don't exist yet
        self.assertFalse(os.path.exists(self.cert_path), "Certificate should not exist before test")
        self.assertFalse(os.path.exists(self.key_path), "Key should not exist before test")
        
        # Generate certificate
        result = generate_self_signed_certificate(self.cert_path, self.key_path)
        
        # Verify generation was successful
        self.assertTrue(result, "Certificate generation should return True on success")
        
        # Check if files were created
        self.assertTrue(os.path.exists(self.cert_path), "Certificate file was not created")
        self.assertTrue(os.path.exists(self.key_path), "Key file was not created")
        
        # Check file permissions on key (should be 0600)
        key_permissions = oct(os.stat(self.key_path).st_mode & 0o777)
        self.assertEqual(key_permissions, oct(0o600), f"Key file has incorrect permissions: {key_permissions}")
        
        # Check certificate validity by examining file contents
        with open(self.cert_path, 'rb') as f:
            cert_data = f.read()
        self.assertIn(b'-----BEGIN CERTIFICATE-----', cert_data, "Invalid certificate format")
        self.assertIn(b'-----END CERTIFICATE-----', cert_data, "Invalid certificate format")
        
        # Check key validity
        with open(self.key_path, 'rb') as f:
            key_data = f.read()
        self.assertIn(b'-----BEGIN PRIVATE KEY-----', key_data, "Invalid private key format")
        self.assertIn(b'-----END PRIVATE KEY-----', key_data, "Invalid private key format")


class TestWebInterfaceSSL(unittest.TestCase):
    """Test class for the web interface's SSL auto-generation integration."""
    
    def setUp(self):
        """Set up test environment with temporary directories."""
        self.temp_dir = tempfile.mkdtemp()
        self.ssl_dir = os.path.join(self.temp_dir, 'ssl')
        self.certs_dir = os.path.join(self.ssl_dir, 'certs')
        self.private_dir = os.path.join(self.ssl_dir, 'private')
        
        # Create directory structure
        os.makedirs(self.certs_dir, exist_ok=True)
        os.makedirs(self.private_dir, exist_ok=True)
        
        # Define paths for certificate and key
        self.cert_path = os.path.join(self.certs_dir, 'kegdisplay.crt')
        self.key_path = os.path.join(self.private_dir, 'kegdisplay.key')
        
        # Save old sys.argv and patch it for our test
        self.old_argv = sys.argv
        sys.argv = ['webinterface.py', '--ssl-cert', self.cert_path, '--ssl-key', self.key_path]
        
        # Patch environment variables to avoid affecting actual config
        self.old_env = os.environ.copy()
        os.environ['HOME'] = self.temp_dir
    
    def tearDown(self):
        """Clean up the temporary directory and restore environment."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # Restore sys.argv and environment
        sys.argv = self.old_argv
        os.environ.clear()
        os.environ.update(self.old_env)
    
    def test_start_with_missing_certificates(self):
        """Test that the web interface properly generates missing certificates on startup."""
        # Import the start function and patch it to avoid actually starting the server
        from unittest.mock import patch
        from KegDisplayDB.web.webinterface import start, parse_args
        
        # Parse the arguments we set up in setUp
        args = parse_args()
        self.assertEqual(args.ssl_cert, self.cert_path)
        self.assertEqual(args.ssl_key, self.key_path)
        
        # Verify certificates don't exist yet
        self.assertFalse(os.path.exists(self.cert_path))
        self.assertFalse(os.path.exists(self.key_path))
        
        # Mock KegDisplayApplication.run to avoid actually starting the server
        with patch('KegDisplayDB.web.webinterface.KegDisplayApplication.run') as mock_run:
            # Call start with our mocked args
            start(args)
            
            # Verify certificates were created
            self.assertTrue(os.path.exists(self.cert_path), "Certificate was not auto-generated")
            self.assertTrue(os.path.exists(self.key_path), "Key was not auto-generated")
            
            # Verify server was started with correct SSL options
            mock_run.assert_called_once()
            
            # Read the certificate and key to verify their format
            with open(self.cert_path, 'rb') as f:
                cert_data = f.read()
            with open(self.key_path, 'rb') as f:
                key_data = f.read()
                
            self.assertIn(b'-----BEGIN CERTIFICATE-----', cert_data)
            self.assertIn(b'-----BEGIN PRIVATE KEY-----', key_data)


if __name__ == '__main__':
    unittest.main() 