# master.py
from flask import Flask, request, jsonify
import threading, time, uuid

app = Flask(__name__)

tasks = {}
task_queue = []
workers = {}
lock = threading.Lock()

# Track makespan across batches
first_task_start = None
last_task_end = None


# -------- Helper Functions --------
def get_summary():
    with lock:
        total = len(tasks)
        queued = sum(1 for t in tasks.values() if t['status'] == 'queued')
        running = sum(1 for t in tasks.values() if t['status'] == 'running')
        done = sum(1 for t in tasks.values() if t['status'] == 'done')
        failed = sum(1 for t in tasks.values() if t['status'] == 'failed')
    return {"total": total, "queued": queued, "running": running, "done": done, "failed": failed}


# -------- API Endpoints --------
@app.route('/submit', methods=['POST'])
def submit_task():
    global first_task_start, last_task_end
    data = request.get_json(force=True)
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "type": data.get("type"),
        "payload": data.get("payload"),
        "priority": data.get("priority", 50),
        "status": "queued",
        "result": None,
        "worker": None,
        "timestamp": time.strftime("%H:%M:%S")
    }
    with lock:
        # Reset timers for new batch
        if len(tasks) == 0:
            first_task_start = None
            last_task_end = None

        tasks[task_id] = task
        task_queue.append(task)
        task_queue.sort(key=lambda t: t["priority"])  # lower = higher priority
    return jsonify({"message": "Task submitted", "task_id": task_id}), 201


@app.route('/get_task', methods=['GET'])
def get_task():
    global first_task_start
    worker_id = request.args.get("worker_id", "unknown")
    with lock:
        if not task_queue:
            return jsonify({"task": None})
        task = task_queue.pop(0)
        task["status"] = "running"
        task["worker"] = worker_id
        task["start_time"] = time.time()
        if first_task_start is None:
            first_task_start = task["start_time"]
    return jsonify({"task": task})


@app.route('/update_task', methods=['POST'])
def update_task():
    global last_task_end
    data = request.get_json(force=True)
    task_id = data.get("id")
    result = data.get("result")
    status = data.get("status", "done")
    with lock:
        if task_id in tasks:
            t = tasks[task_id]
            t["result"] = result
            t["status"] = status
            if status == "done":
                t["end_time"] = time.time()
                last_task_end = t["end_time"]
    return jsonify({"message": "Task updated"}), 200


@app.route('/register_worker', methods=['POST'])
def register_worker():
    data = request.get_json(force=True)
    worker_id = data.get("id")
    with lock:
        workers[worker_id] = {"id": worker_id, "last_seen": time.time()}
    print(f"[MASTER] Registered new worker: {worker_id}")
    return jsonify({"message": "Worker registered"}), 200


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.get_json(force=True)
    worker_id = data.get("id")
    with lock:
        if worker_id in workers:
            workers[worker_id]["last_seen"] = time.time()
    return jsonify({"message": "Heartbeat received"}), 200


# -------- Background Monitor for Fault Tolerance --------
def monitor_workers():
    """Detect dead workers and reassign their tasks automatically."""
    while True:
        time.sleep(5)
        now = time.time()
        with lock:
            dead = []
            for wid, info in list(workers.items()):
                if now - info["last_seen"] > 10:
                    dead.append(wid)
            for wid in dead:
                print(f"\n[⚠️ MASTER] Worker {wid} is unresponsive. Reassigning its tasks...")
                reassigned = 0
                for t in tasks.values():
                    if t["worker"] == wid and t["status"] == "running":
                        t["status"] = "queued"
                        t["worker"] = None
                        task_queue.append(t)
                        reassigned += 1
                del workers[wid]
                print(f"[MASTER] Worker {wid} removed. {reassigned} task(s) requeued.\n")

threading.Thread(target=monitor_workers, daemon=True).start()


# -------- Reset System (for clean batch testing) --------
@app.route('/reset', methods=['POST'])
def reset_system():
    global tasks, task_queue, workers, first_task_start, last_task_end
    with lock:
        tasks.clear()
        task_queue.clear()
        first_task_start = None
        last_task_end = None
    print("[MASTER] System reset. All tasks cleared.")
    return jsonify({"message": "System reset successful"}), 200


# -------- Dashboard --------
@app.route('/status', methods=['GET'])
def status():
    global first_task_start, last_task_end
    summary = get_summary()

    # average completion time
    done_times = [t.get("end_time", 0) - t.get("start_time", 0) for t in tasks.values()
                  if t["status"] == "done" and "start_time" in t and "end_time" in t]
    avg_time = sum(done_times) / len(done_times) if done_times else 0

    # makespan logic
    if summary["running"] == 0 and summary["queued"] == 0 and summary["done"] > 0:
        makespan = (last_task_end - first_task_start) if (first_task_start and last_task_end) else 0
    else:
        makespan = time.time() - first_task_start if first_task_start else 0

    # success/failure rate
    success_rate = (summary['done'] / summary['total'] * 100) if summary['total'] else 0
    fail_rate = (summary['failed'] / summary['total'] * 100) if summary['total'] else 0

    with lock:
        recent_tasks = sorted(tasks.values(), key=lambda t: t["timestamp"], reverse=True)[:30]
        live_workers = len(workers)

        worker_table = "<h3>🧩 Active Workers</h3><table><tr><th>Worker ID</th><th>Last Seen (sec ago)</th></tr>"
        for wid, info in workers.items():
            last_seen = round(time.time() - info['last_seen'], 1)
            worker_table += f"<tr><td>{wid}</td><td>{last_seen}</td></tr>"
        worker_table += "</table>"

    html = f"""
    <html>
    <head>
        <title>Distributed OS - Master Dashboard</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body {{ font-family: Arial, sans-serif; background:#f9fafb; color:#222; margin:30px; }}
            h1 {{ color:#2b2b2b; }}
            table {{ border-collapse: collapse; width: 100%; margin-top:10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            th, td {{ border: 1px solid #ccc; padding: 8px; text-align: center; }}
            th {{ background: #f0f0f0; }}
            tr:nth-child(even) {{ background: #f9f9f9; }}
            .summary {{ background: #eaf5ff; padding: 12px; border-radius: 12px; margin-bottom: 20px; font-size: 16px; }}
            .done {{ color: green; font-weight: bold; }}
            .failed {{ color: red; font-weight: bold; }}
            .running {{ color: orange; font-weight: bold; }}
            .queued {{ color: gray; }}
        </style>
    </head>
    <body>
        <h1>📊 Distributed OS - Master Dashboard</h1>
        <div class="summary">
            <b>Workers:</b> {live_workers} |
            <b>Total:</b> {summary['total']} |
            🕒 <b>Queued:</b> {summary['queued']} |
            ⚙️ <b>Running:</b> {summary['running']} |
            ✅ <b>Done:</b> {summary['done']} |
            ❌ <b>Failed:</b> {summary['failed']} |
            ✅ <b>Success Rate:</b> {success_rate:.1f}% |
            ❌ <b>Failure Rate:</b> {fail_rate:.1f}% |
            ⏱️ <b>Avg Task Time:</b> {avg_time:.2f}s |
            ⏳ <b>Total Makespan:</b> {makespan:.2f}s
        </div>
        {worker_table}
        <h3>📋 Recent Tasks</h3>
        <table>
            <tr>
                <th>ID</th><th>Type</th><th>Priority</th><th>Status</th>
                <th>Worker</th><th>Time</th>
            </tr>
    """
    for t in recent_tasks:
        color_class = t["status"]
        html += f"<tr><td>{t['id'][:6]}</td><td>{t['type']}</td><td>{t['priority']}</td>" \
                f"<td class='{color_class}'>{t['status']}</td><td>{t['worker'] or '-'}</td><td>{t['timestamp']}</td></tr>"
    html += "</table></body></html>"
    return html


# -------- Run Server --------
if __name__ == '__main__':
    print("[MASTER] Server starting on port 5000...")
    app.run(host="0.0.0.0", port=5000)
