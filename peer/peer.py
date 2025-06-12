import socket
import json
import threading
import time
import uuid
import logging
import os
from typing import Optional
from shared.protocol import Protocol

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Peer:
    def __init__(self, discovery_port: int = 9999, tcp_port: int = 7777):
        self.peer_id = str(uuid.uuid4())
        self.discovery_port = discovery_port
        self.tcp_port = tcp_port
        self.master_addr = None
        self.running = False
        self.work_dir = "work"
        
        # Ensure work directory exists
        os.makedirs(self.work_dir, exist_ok=True)
    
    def discover_master(self) -> bool:
        """Discover master using UDP broadcast"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(5)  # 5 seconds timeout
        
        try:
            # Send discovery message
            message = Protocol.discover_master(self.peer_id, self.tcp_port)
            sock.sendto(message.encode(), ('<broadcast>', self.discovery_port))
            
            # Wait for response
            data, addr = sock.recvfrom(1024)
            response = json.loads(data.decode())
            
            if response.get("action") == "MASTER_ANNOUNCE":
                self.master_addr = (response["master_ip"], response["master_port"])
                logging.info(f"Discovered master at {self.master_addr[0]}:{self.master_addr[1]}")
                return True
        except socket.timeout:
            logging.warning("Master discovery timed out")
        except Exception as e:
            logging.error(f"Error during master discovery: {e}")
        finally:
            sock.close()
        
        return False
    
    def register_with_master(self) -> bool:
        """Register with the master"""
        if not self.master_addr:
            return False
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.master_addr)
                
                # Get peer IP (this might need adjustment for NAT environments)
                peer_ip = socket.gethostbyname(socket.gethostname())
                
                message = Protocol.register_peer(
                    peer_id=self.peer_id,
                    addr=[peer_ip, self.tcp_port]
                )
                
                sock.sendall(message.encode())
                response = json.loads(sock.recv(1024).decode())
                
                if response.get("status") == "REGISTERED":
                    logging.info("Successfully registered with master")
                    return True
        except Exception as e:
            logging.error(f"Error registering with master: {e}")
        
        return False
    
    def send_heartbeat(self):
        """Send heartbeat to master"""
        if not self.master_addr:
            return False
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.master_addr)
                
                message = Protocol.heartbeat(self.peer_id)
                sock.sendall(message.encode())
                
                response = json.loads(sock.recv(1024).decode())
                return response.get("status") == "ALIVE"
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")
            return False
    
    def request_task(self) -> Optional[dict]:
        """Request a task from master"""
        if not self.master_addr:
            return None
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.master_addr)
                
                message = Protocol.request_task(self.peer_id)
                sock.sendall(message.encode())
                
                response = json.loads(sock.recv(65536).decode())  # Larger buffer for task data
                
                if response.get("action") == "TASK_PACKAGE":
                    return {
                        "task_name": response["task_name"],
                        "task_data": response["task_data"]
                    }
                elif response.get("status") == "NO_TASKS":
                    logging.info("No tasks available from master")
        except Exception as e:
            logging.error(f"Error requesting task: {e}")
        
        return None
    
    def submit_result(self, result_name: str, result_data: bytes) -> bool:
        """Submit task result to master"""
        if not self.master_addr:
            return False
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.master_addr)
                
                message = Protocol.submit_result(
                    peer_id=self.peer_id,
                    result_name=result_name,
                    result_data=result_data
                )
                
                sock.sendall(message.encode())
                response = json.loads(sock.recv(1024).decode())
                
                return response.get("status") == "OK"
        except Exception as e:
            logging.error(f"Error submitting result: {e}")
            return False
    
    def start_heartbeat(self, interval: int = 10):
        """Start periodic heartbeat to master"""
        while self.running:
            if self.master_addr:
                self.send_heartbeat()
            time.sleep(interval)
    
    def start_worker(self):
        """Start worker process to handle tasks"""
        while self.running:
            if self.master_addr:
                task = self.request_task()
                
                if task:
                    from peer.worker import Worker
                    worker = Worker(self.work_dir)
                    result = worker.process_task(task)
                    
                    if result:
                        self.submit_result(result["result_name"], result["result_data"])
                else:
                    time.sleep(5)  # Wait before requesting again if no tasks
            else:
                time.sleep(1)
    
    def start(self):
        """Start peer services"""
        self.running = True
        
        # Discover master
        if not self.discover_master():
            logging.error("Failed to discover master")
            return
        
        # Register with master
        if not self.register_with_master():
            logging.error("Failed to register with master")
            return
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self.start_heartbeat,
            daemon=True
        )
        heartbeat_thread.start()
        
        # Start worker thread
        worker_thread = threading.Thread(
            target=self.start_worker,
            daemon=True
        )
        worker_thread.start()
        
        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False

if __name__ == "__main__":
    peer = Peer()
    peer.start()