# worker.py (v2 - with heartbeat)
import requests, time, sys, uuid, threading

MASTER_URL = "http://127.0.0.1:8000"
WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else f"worker-{uuid.uuid4().hex[:4]}"

def heartbeat_loop():
    while True:
        try:
            requests.post(f"{MASTER_URL}/heartbeat",
                          json={"worker_id": WORKER_ID, "timestamp": time.time()},
                          timeout=2)
        except:
            pass
        time.sleep(5)

def do_sort(arr): return sorted(arr)

def do_matmul(A, B):
    n, m, p = len(A), len(B), len(B[0])
    C = [[0]*p for _ in range(n)]
    for i in range(n):
        for j in range(p):
            for k in range(m):
                C[i][j] += A[i][k] * B[k][j]
    return C

def task_loop():
    while True:
        try:
            r = requests.get(f"{MASTER_URL}/get_task", params={"worker_id": WORKER_ID})
            task = r.json().get("task")
            if not task:
                print(f"[{WORKER_ID}] no task, waiting...")
                time.sleep(2)
                continue
            tid, ttype, payload = task["task_id"], task["type"], task["payload"]
            print(f"[{WORKER_ID}] got task {tid} ({ttype})")
            if ttype == "sort":
                result = {"sorted": do_sort(payload["array"])}
            elif ttype == "matmul":
                result = {"C": do_matmul(payload["A"], payload["B"])}
            else:
                result = {"error": "unknown type"}
            requests.post(f"{MASTER_URL}/submit_result",
                          json={"task_id": tid, "status": "done",
                                "result": result, "worker_id": WORKER_ID})
            print(f"[{WORKER_ID}] finished {tid}")
        except Exception as e:
            print(f"[{WORKER_ID}] error: {e}")
            time.sleep(3)

if __name__ == "__main__":
    print(f"Starting worker {WORKER_ID}")
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    task_loop()
