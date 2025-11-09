# worker.py
import requests, time, uuid, threading, sys
from tasks import run_task


def worker_main(master_url="http://127.0.0.1:5000"):
    wid = f"worker-{uuid.uuid4().hex[:6]}"
    print(f"[WORKER] Started {wid}, connected to {master_url}")

    # ---- Register worker ----
    try:
        res = requests.post(f"{master_url}/register_worker", json={"id": wid}, timeout=5)
        if res.status_code == 200:
            print(f"[WORKER {wid}] Registered with master.")
        else:
            print(f"[WORKER {wid}] Registration failed: {res.text}")
    except Exception as e:
        print(f"[WORKER {wid}] Could not register: {e}")

    # ---- Send heartbeat every 3 seconds ----
    def heartbeat():
        while True:
            try:
                requests.post(f"{master_url}/heartbeat", json={"id": wid}, timeout=3)
            except Exception:
                pass
            time.sleep(3)

    threading.Thread(target=heartbeat, daemon=True).start()

    # ---- Main polling and execution loop ----
    while True:
        try:
            # Ask master for a new task
            r = requests.get(f"{master_url}/get_task", params={"worker_id": wid}, timeout=5)
            if r.status_code != 200:
                time.sleep(1)
                continue

            task = r.json().get("task")
            if not task:
                time.sleep(1)
                continue

            print(f"[WORKER {wid}] Executing {task['id']} ({task['type']})")

            try:
                # Execute the task
                result = run_task(task["type"], task["payload"])

                # Report completion
                requests.post(f"{master_url}/update_task",
                              json={"id": task["id"], "result": result, "status": "done"},
                              timeout=5)

                print(f"[WORKER {wid}] ✅ Task {task['id']} completed.")
            except Exception as e:
                # Report failure
                print(f"[WORKER {wid}] ❌ Error executing task: {e}")
                requests.post(f"{master_url}/update_task",
                              json={"id": task["id"], "result": str(e), "status": "failed"},
                              timeout=5)

        except Exception as e:
            print(f"[WORKER {wid}] Connection error: {e}")
            time.sleep(3)


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
    worker_main(url)
