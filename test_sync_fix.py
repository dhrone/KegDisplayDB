#!/usr/bin/env python3
"""
Test script to verify the sync service fix.
This script tests that the DBSyncService properly starts network listeners.
"""

import time
import socket
import subprocess
import sys
import signal
import os
from pathlib import Path

def test_port_binding(port, protocol="UDP"):
    """Test if a port is being listened on"""
    try:
        if protocol == "UDP":
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(('127.0.0.1', port))
            sock.close()
            return False  # Port is free
        else:  # TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', port))
            sock.close()
            return result == 0  # Port is in use
    except:
        return True  # Port is likely in use

def check_process_ports():
    """Check what ports the running dbsync process is using"""
    try:
        # Check for UDP port 5002
        result = subprocess.run(['netstat', '-ulnp'], capture_output=True, text=True)
        udp_5002_bound = ':5002 ' in result.stdout
        
        # Check for TCP port 5003  
        result = subprocess.run(['netstat', '-tlnp'], capture_output=True, text=True)
        tcp_5003_bound = ':5003 ' in result.stdout
        
        return udp_5002_bound, tcp_5003_bound
        
    except Exception as e:
        print(f"Error checking ports: {e}")
        return False, False

def main():
    print("Testing DBSyncService network binding fix...")
    print("=" * 50)
    
    # First check if ports are currently in use
    print("1. Checking initial port status...")
    udp_bound, tcp_bound = check_process_ports()
    print(f"   UDP port 5002: {'✓ BOUND' if udp_bound else '✗ NOT BOUND'}")
    print(f"   TCP port 5003: {'✓ BOUND' if tcp_bound else '✗ NOT BOUND'}")
    
    if udp_bound and tcp_bound:
        print("\n✓ SUCCESS: Both ports are already bound!")
        print("The fix appears to be working correctly.")
        return True
    
    print(f"\n2. Current working directory: {os.getcwd()}")
    
    # Check if we're in the right directory
    if not Path("KegDisplayDB").exists():
        print("✗ ERROR: KegDisplayDB directory not found.")
        print("Please run this script from the project root directory.")
        return False
    
    print("\n3. Testing the fix by running dbsync for 10 seconds...")
    
    # Start dbsync process
    try:
        process = subprocess.Popen([
            'poetry', 'run', 'dbsync', '--log-level', 'debug'
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        print("   Started dbsync process...")
        
        # Wait a moment for startup
        time.sleep(3)
        
        # Check if process is still running
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            print("✗ ERROR: dbsync process exited early")
            print("STDOUT:", stdout[-500:])  # Last 500 chars
            print("STDERR:", stderr[-500:])
            return False
        
        # Check ports again
        print("   Checking ports after startup...")
        udp_bound, tcp_bound = check_process_ports()
        print(f"   UDP port 5002: {'✓ BOUND' if udp_bound else '✗ NOT BOUND'}")
        print(f"   TCP port 5003: {'✓ BOUND' if tcp_bound else '✗ NOT BOUND'}")
        
        # Terminate the process
        print("   Stopping dbsync process...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        
        if udp_bound and tcp_bound:
            print("\n✓ SUCCESS: The fix is working!")
            print("The dbsync service now properly binds to network ports.")
            return True
        else:
            print("\n✗ FAILURE: Ports are still not bound.")
            print("The fix may not be complete or there's another issue.")
            
            # Get process output for debugging
            stdout, stderr = process.communicate()
            print("\nProcess output (last 1000 chars):")
            print("STDOUT:", stdout[-1000:])
            print("STDERR:", stderr[-1000:])
            return False
            
    except Exception as e:
        print(f"✗ ERROR: Failed to test dbsync: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 