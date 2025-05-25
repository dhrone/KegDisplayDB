#!/usr/bin/env python3
"""
Network diagnostic script for KegDisplayDB sync issues.
This script tests UDP socket binding and basic network functionality.
"""

import socket
import threading
import time
import sys
import logging
import json

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_udp_bind(port=5002):
    """Test if we can bind to the UDP broadcast port"""
    logger.info(f"Testing UDP socket binding on port {port}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        logger.info(f"Created UDP socket, attempting to bind to port {port}")
        sock.bind(('', port))
        logger.info(f"✓ Successfully bound UDP socket to port {port}")
        
        # Test receiving
        sock.settimeout(2.0)
        logger.info("Testing socket receive (2 second timeout)...")
        
        try:
            data, addr = sock.recvfrom(1024)
            logger.info(f"Received data from {addr}: {data[:50]}")
        except socket.timeout:
            logger.info("No data received (timeout - this is normal for test)")
        except Exception as e:
            logger.error(f"Error receiving data: {e}")
        
        sock.close()
        logger.info("UDP socket test completed successfully")
        return True
        
    except PermissionError as e:
        logger.error(f"✗ Permission denied binding to port {port}: {e}")
        logger.error("Try running as root or use a port > 1024")
        return False
    except OSError as e:
        if e.errno == 98:  # Address already in use
            logger.error(f"✗ Port {port} is already in use")
            logger.error("Another process may be using this port")
        else:
            logger.error(f"✗ OS error binding to port {port}: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Unexpected error binding to port {port}: {e}")
        return False

def test_tcp_bind(port=5003):
    """Test if we can bind to the TCP sync port"""
    logger.info(f"Testing TCP socket binding on port {port}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        logger.info(f"Created TCP socket, attempting to bind to port {port}")
        sock.bind(('', port))
        sock.listen(1)
        logger.info(f"✓ Successfully bound TCP socket to port {port}")
        
        sock.close()
        logger.info("TCP socket test completed successfully")
        return True
        
    except PermissionError as e:
        logger.error(f"✗ Permission denied binding to port {port}: {e}")
        return False
    except OSError as e:
        if e.errno == 98:  # Address already in use
            logger.error(f"✗ Port {port} is already in use")
        else:
            logger.error(f"✗ OS error binding to port {port}: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Unexpected error binding to port {port}: {e}")
        return False

def get_local_ips():
    """Get local IP addresses"""
    logger.info("Discovering local IP addresses")
    
    local_ips = ['127.0.0.1', '127.0.1.1']
    
    try:
        # Primary method - connect to external address
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            if ip not in local_ips:
                local_ips.append(ip)
                logger.info(f"Found primary IP: {ip}")
        
        # Hostname method
        hostname = socket.gethostname()
        try:
            ip = socket.gethostbyname(hostname)
            if ip not in local_ips:
                local_ips.append(ip)
                logger.info(f"Found hostname IP: {ip}")
        except:
            pass
            
    except Exception as e:
        logger.warning(f"Error discovering IPs: {e}")
    
    logger.info(f"Local IPs: {local_ips}")
    return local_ips

def test_broadcast_send(port=5002):
    """Test sending a broadcast message"""
    logger.info(f"Testing broadcast send on port {port}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        test_message = json.dumps({
            "type": "test",
            "message": "Network diagnostic test",
            "timestamp": time.time()
        }).encode('utf-8')
        
        sock.sendto(test_message, ('<broadcast>', port))
        logger.info(f"✓ Successfully sent broadcast message to port {port}")
        
        sock.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Error sending broadcast: {e}")
        return False

def test_broadcast_listener(port=5002, duration=10):
    """Test listening for broadcast messages"""
    logger.info(f"Testing broadcast listener on port {port} for {duration} seconds")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', port))
        sock.settimeout(1.0)
        
        logger.info(f"✓ Listening for broadcasts on port {port}")
        
        start_time = time.time()
        received_count = 0
        
        while time.time() - start_time < duration:
            try:
                data, addr = sock.recvfrom(1024)
                received_count += 1
                logger.info(f"Received broadcast #{received_count} from {addr[0]}: {len(data)} bytes")
                
                # Try to parse as JSON
                try:
                    msg = json.loads(data.decode('utf-8'))
                    logger.info(f"  Message type: {msg.get('type', 'unknown')}")
                except:
                    logger.info(f"  Raw data: {data[:50]}")
                    
            except socket.timeout:
                continue
            except Exception as e:
                logger.warning(f"Error receiving broadcast: {e}")
        
        sock.close()
        logger.info(f"Broadcast listener test completed. Received {received_count} messages.")
        return received_count > 0
        
    except Exception as e:
        logger.error(f"✗ Error in broadcast listener: {e}")
        return False

def main():
    """Run all network diagnostic tests"""
    logger.info("Starting KegDisplayDB network diagnostic tests")
    logger.info("=" * 60)
    
    # Test local IP discovery
    local_ips = get_local_ips()
    
    # Test UDP binding
    logger.info("\n" + "=" * 60)
    udp_ok = test_udp_bind()
    
    # Test TCP binding  
    logger.info("\n" + "=" * 60)
    tcp_ok = test_tcp_bind()
    
    # Test broadcast send
    logger.info("\n" + "=" * 60)
    send_ok = test_broadcast_send()
    
    # Test broadcast receive
    logger.info("\n" + "=" * 60)
    if udp_ok:
        logger.info("UDP binding works, testing broadcast listener...")
        receive_ok = test_broadcast_listener(duration=5)
    else:
        logger.warning("Skipping broadcast listener test due to UDP binding failure")
        receive_ok = False
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSTIC SUMMARY:")
    logger.info(f"Local IPs discovered: {len(local_ips)}")
    logger.info(f"UDP port 5002 binding: {'✓ PASS' if udp_ok else '✗ FAIL'}")
    logger.info(f"TCP port 5003 binding: {'✓ PASS' if tcp_ok else '✗ FAIL'}")  
    logger.info(f"Broadcast sending: {'✓ PASS' if send_ok else '✗ FAIL'}")
    logger.info(f"Broadcast receiving: {'✓ PASS' if receive_ok else '✗ FAIL'}")
    
    if not udp_ok:
        logger.error("\nUDP binding failed - this is the likely cause of the sync issue!")
        logger.error("Possible solutions:")
        logger.error("1. Check if another process is using port 5002")
        logger.error("2. Run with different ports using --broadcast-port")
        logger.error("3. Check firewall settings")
        logger.error("4. Try running as root (if permission error)")
    elif not receive_ok:
        logger.warning("\nUDP binding works but no broadcasts received.")
        logger.warning("This could be normal if no other systems are broadcasting.")
    else:
        logger.info("\nAll network tests passed!")
    
    return udp_ok and tcp_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 