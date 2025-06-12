import base64
import socket
import threading
import json
import os
import logging
from typing import Dict, Tuple
from shared.protocol import Protocol
from master.peer_manager import PeerManager
from master.task_manager import TaskManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Master:
    def __init__(self, discovery_port: int = 9999, tcp_port: int = 8888):
        self.discovery_port = discovery_port
        self.tcp_port = tcp_port
        self.peer_manager = PeerManager()
        self.task_manager = TaskManager()
        self.running = False
        
        # Ensure directories exist
        os.makedirs("tasks", exist_ok=True)
        os.makedirs("results", exist_ok=True)
    
    def start_discovery_service(self):
        """Start UDP service for peer discovery"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(('', self.discovery_port))
        
        logging.info(f"Discovery service running on port {self.discovery_port}")
        
        while self.running:
            try:
                data, addr = sock.recvfrom(1024)
                message = json.loads(data.decode())
                
                if message.get("action") == "DISCOVER_MASTER":
                    peer_id = message["peer_id"]
                    port_tcp = message["port_tcp"]
                    
                    logging.info(f"Received discovery request from {addr[0]}:{port_tcp}")
                    
                    response = Protocol.master_announce(
                        master_ip=socket.gethostbyname(socket.gethostname()),
                        master_port=self.tcp_port
                    )
                    
                    sock.sendto(response.encode(), addr)
            except Exception as e:
                logging.error(f"Error in discovery service: {e}")
    
    def handle_peer_connection(self, conn, addr):
        """Handle a single peer TCP connection"""
        try:
            with conn:
                while self.running:
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    try:
                        message = json.loads(data.decode())
                        action = message.get("action")
                        
                        if action == "REGISTER":
                            peer_id = message["peer_id"]
                            peer_addr = message["addr"]
                            self.peer_manager.register_peer(peer_id, peer_addr)
                            response = Protocol.registration_response("REGISTERED")
                            conn.sendall(response.encode())
                        
                        elif action == "HEARTBEAT":
                            peer_id = message["peer_id"]
                            self.peer_manager.update_peer_heartbeat(peer_id)
                            response = Protocol.heartbeat_response("ALIVE")
                            conn.sendall(response.encode())
                        
                        elif action == "REQUEST_TASK":
                            peer_id = message["peer_id"]
                            task = self.task_manager.get_next_task()
                            
                            if task:
                                with open(task, 'rb') as f:
                                    task_data = f.read()
                                task_name = os.path.basename(task)
                                response = Protocol.task_package(task_name, task_data)
                                conn.sendall(response.encode())
                            else:
                                response = json.dumps({"status": "NO_TASKS"})
                                conn.sendall(response.encode())
                        
                        elif action == "SUBMIT_RESULT":
                            peer_id = message["peer_id"]
                            result_name = message["result_name"]
                            result_data = base64.b64decode(message["result_data"])
                            
                            result_path = os.path.join("results", result_name)
                            with open(result_path, 'wb') as f:
                                f.write(result_data)
                            
                            logging.info(f"Received result {result_name} from peer {peer_id}")
                            response = Protocol.result_response("OK")
                            conn.sendall(response.encode())
                    
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
                        response = json.dumps({"status": "ERROR", "message": str(e)})
                        conn.sendall(response.encode())
        
        except Exception as e:
            logging.error(f"Error handling peer connection {addr}: {e}")
    
    def start_tcp_service(self):
        """Start TCP service for peer communication"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', self.tcp_port))
        sock.listen()
        
        logging.info(f"TCP service running on port {self.tcp_port}")
        
        while self.running:
            try:
                conn, addr = sock.accept()
                threading.Thread(
                    target=self.handle_peer_connection,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except Exception as e:
                logging.error(f"Error in TCP service: {e}")
    
    def start(self):
        """Start master services"""
        self.running = True
        
        # Start discovery service in a separate thread
        discovery_thread = threading.Thread(
            target=self.start_discovery_service,
            daemon=True
        )
        discovery_thread.start()
        
        # Start TCP service in the main thread
        self.start_tcp_service()
    
    def stop(self):
        """Stop master services"""
        self.running = False

if __name__ == "__main__":
    master = Master()
    try:
        master.start()
    except KeyboardInterrupt:
        master.stop()