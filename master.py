# master.py (v2 - with fault tolerance)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import heapq, time, threading
from typing import Optional, Dict

app = FastAPI(title="Distributed Task Scheduler")

# -------- Data models --------
class Task(BaseModel):
    task_id: str
    type: str
    payload: dict
    priority: int = 10

class Result(BaseModel):
    task_id: str
    status: str
    result: Optional[dict] = None
    worker_id: Optional[str] = None

class Heartbeat(BaseModel):
    worker_id: str
    timestamp: float

# -------- Data structures --------
task_heap = []                     # queue of (priority, ts, id)
task_data = {}                     # task_id -> task info
assigned = {}                      # task_id -> (worker_id, assigned_time)
workers: Dict[str, float] = {}     # worker_id -> last heartbeat

TASK_TIMEOUT = 15                  # seconds after which task is requeued
HEARTBEAT_TIMEOUT = 10             # seconds after which worker is considered dead
lock = threading.Lock()

# -------- APIs --------
@app.post("/submit_task")
def submit_task(t: Task):
    with lock:
        if t.task_id in task_data:
            raise HTTPException(status_code=400, detail="Task ID already exists")
        heapq.heappush(task_heap, (t.priority, time.time(), t.task_id))
        task_data[t.task_id] = {"type": t.type, "payload": t.payload, "priority": t.priority, "state": "queued"}
    return {"status": "accepted", "task_id": t.task_id}

@app.get("/get_task")
def get_task(worker_id: str):
    with lock:
        workers[worker_id] = time.time()  # update heartbeat
        if not task_heap:
            return {"task": None}
        pr, ts, tid = heapq.heappop(task_heap)
        task_data[tid]["state"] = "assigned"
        assigned[tid] = (worker_id, time.time())
        return {"task": {"task_id": tid, "type": task_data[tid]["type"], "payload": task_data[tid]["payload"]}}

@app.post("/submit_result")
def submit_result(r: Result):
    with lock:
        if r.task_id not in task_data:
            raise HTTPException(status_code=404, detail="Unknown task")
        task_data[r.task_id]["state"] = r.status
        task_data[r.task_id]["result"] = r.result
        if r.task_id in assigned:
            del assigned[r.task_id]
    return {"status": "ok"}

@app.post("/heartbeat")
def heartbeat(h: Heartbeat):
    with lock:
        workers[h.worker_id] = h.timestamp
    return {"status": "alive"}

@app.get("/status")
def status():
    with lock:
        return {
            "queued": sum(1 for t in task_data.values() if t["state"]=="queued"),
            "assigned": sum(1 for t in task_data.values() if t["state"]=="assigned"),
            "done": sum(1 for t in task_data.values() if t["state"]=="done"),
            "failed": sum(1 for t in task_data.values() if t["state"]=="failed"),
            "workers": list(workers.keys())
        }

# -------- Background monitoring --------
def monitor():
    while True:
        time.sleep(5)
        now = time.time()
        with lock:
            # Check dead workers
            dead = [wid for wid, ts in workers.items() if now - ts > HEARTBEAT_TIMEOUT]
            for wid in dead:
                print(f"[MASTER] Worker {wid} missed heartbeat -> removing")
                del workers[wid]

            # Requeue old tasks
            for tid, (wid, assigned_time) in list(assigned.items()):
                if now - assigned_time > TASK_TIMEOUT:
                    print(f"[MASTER] Task {tid} from {wid} timed out -> requeued")
                    info = task_data[tid]
                    info["state"] = "queued"
                    heapq.heappush(task_heap, (info["priority"], now, tid))
                    del assigned[tid]

threading.Thread(target=monitor, daemon=True).start()
