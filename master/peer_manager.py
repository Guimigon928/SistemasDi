import time
from typing import Dict, List, Optional

class PeerManager:
    def __init__(self):
        self.peers: Dict[str, dict] = {}
        self.heartbeat_timeout = 30  # seconds
    
    def register_peer(self, peer_id: str, addr: List[str]) -> bool:
        """Register a new peer"""
        if peer_id not in self.peers:
            self.peers[peer_id] = {
                "addr": addr,
                "last_heartbeat": time.time(),
                "active": True
            }
            return True
        return False
    
    def update_peer_heartbeat(self, peer_id: str) -> bool:
        """Update peer's last heartbeat time"""
        if peer_id in self.peers:
            self.peers[peer_id]["last_heartbeat"] = time.time()
            self.peers[peer_id]["active"] = True
            return True
        return False
    
    def check_inactive_peers(self) -> List[str]:
        """Check for peers that haven't sent heartbeat"""
        current_time = time.time()
        inactive_peers = []
        
        for peer_id, peer_data in self.peers.items():
            if (current_time - peer_data["last_heartbeat"]) > self.heartbeat_timeout:
                if peer_data["active"]:
                    peer_data["active"] = False
                    inactive_peers.append(peer_id)
        
        return inactive_peers
    
    def get_active_peers(self) -> Dict[str, dict]:
        """Get all active peers"""
        return {pid: data for pid, data in self.peers.items() if data["active"]}