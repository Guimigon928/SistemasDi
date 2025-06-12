import json
import base64
import uuid
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

@dataclass
class Message:
    action: str
    data: Dict[str, Any]
    
    def to_json(self) -> str:
        return json.dumps({"action": self.action, **self.data})
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        data = json.loads(json_str)
        action = data.pop("action")
        return cls(action=action, data=data)

class Protocol:
    # Discovery messages
    @staticmethod
    def discover_master(peer_id: str, port_tcp: int) -> str:
        return json.dumps({
            "action": "DISCOVER_MASTER",
            "peer_id": peer_id,
            "port_tcp": port_tcp
        })
    
    @staticmethod
    def master_announce(master_ip: str, master_port: int) -> str:
        return json.dumps({
            "action": "MASTER_ANNOUNCE",
            "master_ip": master_ip,
            "master_port": master_port
        })
    
    # Registration messages
    @staticmethod
    def register_peer(peer_id: str, addr: List[str]) -> str:
        return json.dumps({
            "action": "REGISTER",
            "peer_id": peer_id,
            "addr": addr
        })
    
    @staticmethod
    def registration_response(status: str) -> str:
        return json.dumps({"status": status})
    
    # Heartbeat messages
    @staticmethod
    def heartbeat(peer_id: str) -> str:
        return json.dumps({
            "action": "HEARTBEAT",
            "peer_id": peer_id
        })
    
    @staticmethod
    def heartbeat_response(status: str) -> str:
        return json.dumps({"status": status})
    
    # Task messages
    @staticmethod
    def request_task(peer_id: str) -> str:
        return json.dumps({
            "action": "REQUEST_TASK",
            "peer_id": peer_id
        })
    
    @staticmethod
    def task_package(task_name: str, task_data: bytes) -> str:
        return json.dumps({
            "action": "TASK_PACKAGE",
            "task_name": task_name,
            "task_data": base64.b64encode(task_data).decode('utf-8')
        })
    
    @staticmethod
    def submit_result(peer_id: str, result_name: str, result_data: bytes) -> str:
        return json.dumps({
            "action": "SUBMIT_RESULT",
            "peer_id": peer_id,
            "result_name": result_name,
            "result_data": base64.b64encode(result_data).decode('utf-8')
        })
    
    @staticmethod
    def result_response(status: str) -> str:
        return json.dumps({"status": status})