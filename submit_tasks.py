# submit_tasks.py
import requests

MASTER_URL = "http://127.0.0.1:8000"

# Task 1: sorting
requests.post(f"{MASTER_URL}/submit_task", json={
    "task_id": "t1",
    "type": "sort",
    "payload": {"array": [5, 2, 9, 1, 3]},
    "priority": 5
})

# Task 2: matrix multiply
requests.post(f"{MASTER_URL}/submit_task", json={
    "task_id": "t2",
    "type": "matmul",
    "payload": {"A": [[1, 2], [3, 4]], "B": [[5, 6], [7, 8]]},
    "priority": 1
})
