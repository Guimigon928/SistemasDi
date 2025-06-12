import os
import glob
from typing import Optional

class TaskManager:
    def __init__(self, tasks_dir: str = "tasks"):
        self.tasks_dir = tasks_dir
        os.makedirs(self.tasks_dir, exist_ok=True)
    
    def add_task(self, task_path: str) -> bool:
        """Add a new task to the queue"""
        try:
            task_name = os.path.basename(task_path)
            dest_path = os.path.join(self.tasks_dir, task_name)
            
            if os.path.exists(task_path):
                if not os.path.exists(dest_path):
                    os.rename(task_path, dest_path)
                    return True
        except Exception:
            pass
        return False
    
    def get_next_task(self) -> Optional[str]:
        """Get the next available task"""
        tasks = glob.glob(os.path.join(self.tasks_dir, "*.zip"))
        return tasks[0] if tasks else None
    
    def has_tasks(self) -> bool:
        """Check if there are pending tasks"""
        return len(glob.glob(os.path.join(self.tasks_dir, "*.zip"))) > 0