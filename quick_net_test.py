#!/usr/bin/env python3

import socket
import sys

def test_ports():
    print("Testing network ports for KegDisplayDB sync...")
    
    # Test UDP port 5002
    print(f"\n1. Testing UDP port 5002 (broadcast):")
    try:
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.bind(('', 5002))
        print("   ✓ UDP port 5002 binding: SUCCESS")
        udp_sock.close()
        udp_ok = True
    except Exception as e:
        print(f"   ✗ UDP port 5002 binding: FAILED - {e}")
        udp_ok = False
    
    # Test TCP port 5003  
    print(f"\n2. Testing TCP port 5003 (sync):")
    try:
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(('', 5003))
        tcp_sock.listen(1)
        print("   ✓ TCP port 5003 binding: SUCCESS")
        tcp_sock.close()
        tcp_ok = True
    except Exception as e:
        print(f"   ✗ TCP port 5003 binding: FAILED - {e}")
        tcp_ok = False
    
    # Get local IP
    print(f"\n3. Local IP detection:")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        local_ip = s.getsockname()[0]
        s.close()
        print(f"   Primary IP: {local_ip}")
    except Exception as e:
        print(f"   Error getting IP: {e}")
        local_ip = "unknown"
    
    print(f"\n" + "="*50)
    print("SUMMARY:")
    print(f"UDP port 5002: {'✓ OK' if udp_ok else '✗ FAIL'}")
    print(f"TCP port 5003: {'✓ OK' if tcp_ok else '✗ FAIL'}")
    print(f"Local IP: {local_ip}")
    
    if not udp_ok:
        print(f"\n⚠️  UDP port 5002 binding failed!")
        print("This explains why the new system can't receive broadcasts.")
        print("Possible causes:")
        print("- Another process is using port 5002")
        print("- Permission issue (try as root)")
        print("- Firewall blocking the port")
        print("\nTo fix:")
        print("1. Stop the dbsync service: sudo systemctl stop dbsync")
        print("2. Check what's using the port: sudo netstat -ulnp | grep 5002")
        print("3. Restart dbsync: sudo systemctl start dbsync")
    
    return udp_ok and tcp_ok

if __name__ == "__main__":
    success = test_ports()
    sys.exit(0 if success else 1) 