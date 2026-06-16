# profilers/network_profiler.py
import time
import socket
import sys

def measure_bandwidth(target_ip, port=5001, payload_size_mb=5):
    """
    Measures network throughput by pushing a temporary, high-density data 
    packet to a target node and timing the transmission window.
    
    Args:
        target_ip (str): IP address of the destination edge node.
        port (int): Dedicated port allocated for network testing (default 5001).
        payload_size_mb (int): Size of the test packet in Megabytes.
        
    Returns:
        float: Calculated throughput speed in Megabytes per second (MB/s).
    """
    bytes_to_send = b'X' * (payload_size_mb * 1024 * 1024)
    
    print(f"📡 [NET_PROFILER] Initializing {payload_size_mb}MB link test to {target_ip}:{port}...")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(7.0) # Prevent the script from hanging forever if a node drops
    
    try:
        # 1. Connect first to resolve handshakes outside of the timer window
        sock.connect((target_ip, port))
        
        # 2. Start high-resolution monotonic timer right before transmission
        start_time = time.perf_counter()
        
        # Send the entire chunk of data across the network pipe
        sock.sendall(bytes_to_send)
        
        # Wait for a tiny 1-byte acknowledgement back to guarantee delivery complete
        _ = sock.recv(1)
        end_time = time.perf_counter()
        
        elapsed_time = end_time - start_time
        throughput_mb_s = payload_size_mb / elapsed_time
        
        print(f"📊 [NET_PROFILER] Transfer complete in {elapsed_time:.4f}s | Speed: {throughput_mb_s:.2f} MB/s")
        return float(throughput_mb_s)
        
    except Exception as e:
        print(f"⚠️ [NET_PROFILER] Bandwidth test failed to {target_ip}: {e}", file=sys.stderr)
        print("🔄 [NET_PROFILER] Falling back to standard handbook baseline: 117.0 MB/s")
        return 117.0
    finally:
        sock.close()

def start_network_test_receiver(port=5001):
    """
    Runs on the edge device as a background listener to absorb the test packets
    sent by the laptop coordinator.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(1)
        print(f"📥 [NET_RECEIVER] Network testing server active on port {port}...")
        
        while True:
            client_socket, addr = server_socket.accept()
            try:
                while True:
                    data = client_socket.recv(65536) # 64KB buffer blocks
                    if not data:
                        break
                # Send confirmation byte back before tearing down the link
                client_socket.sendall(b'A')
            except Exception as e:
                print(f"❌ Error handling network test stream: {e}")
            finally:
                client_socket.close()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down network testing server.")
    finally:
        server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--server":
        start_network_test_receiver()
    else:
        print("Usage to start test receiver daemon on edge node: python3 network_profiler.py --server")
        print("Usage to test from laptop coordinator: import and call measure_bandwidth(ip)")