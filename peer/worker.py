import os
import base64
import zipfile
import subprocess
import logging
import uuid
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Worker:
    def __init__(self, work_dir: str = "work"):
        self.work_dir = work_dir
        os.makedirs(self.work_dir, exist_ok=True)
    
    def process_task(self, task: Dict) -> Optional[Dict]:
        """Process a task received from master"""
        task_name = task["task_name"]
        task_data = base64.b64decode(task["task_data"])
        
        # Create task directory
        task_id = str(uuid.uuid4())
        task_dir = os.path.join(self.work_dir, task_id)
        os.makedirs(task_dir, exist_ok=True)
        
        # Save and extract task zip
        zip_path = os.path.join(task_dir, task_name)
        with open(zip_path, 'wb') as f:
            f.write(task_data)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(task_dir)
            
            # Execute main.py
            main_script = os.path.join(task_dir, "main.py")
            input_csv = os.path.join(task_dir, "input.csv")
            stdout_file = os.path.join(task_dir, "stdout.txt")
            stderr_file = os.path.join(task_dir, "stderr.txt")
            
            with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
                subprocess.run(
                    ['python', main_script, input_csv],
                    stdout=out,
                    stderr=err,
                    cwd=task_dir,
                    check=True
                )
            
            # Create result zip
            result_name = f"result_{task_id}.zip"
            result_path = os.path.join(self.work_dir, result_name)
            
            with zipfile.ZipFile(result_path, 'w') as zipf:
                for root, _, files in os.walk(task_dir):
                    for file in files:
                        if file.endswith('.txt'):  # Only include output files
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, task_dir)
                            zipf.write(file_path, arcname)
            
            # Read result data
            with open(result_path, 'rb') as f:
                result_data = f.read()
            
            # Clean up
            os.remove(zip_path)
            
            return {
                "result_name": result_name,
                "result_data": result_data
            }
        
        except Exception as e:
            logging.error(f"Error processing task: {e}")
            return None