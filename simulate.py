# simulate.py
"""
Simulation script to compare sequential vs parallel execution of tasks
(using your tasks.py run_task).

Usage:
    python simulate.py --tasks 200 --repeat 3 --workers 1 2 4 8 --out results.csv --plot

Requires:
    - Python 3.8+
    - tasks.py in same folder (must expose run_task(task_type, payload))
    - matplotlib (optional, only for --plot)

Produces:
    - CSV file with aggregated results per scenario
    - Optional plots if --plot provided
"""

import time
import argparse
import csv
import random
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from statistics import mean
from collections import defaultdict

# Import your run_task function from tasks.py
from tasks import run_task

# -------------------- workload generation --------------------
def gen_random_task():
    """Return a (task_type, payload) tuple randomly selected."""
    # Add any types that your tasks.py implements
    TYPES = [
        ("sort", lambda: {"array": [random.randint(1, 100) for _ in range(random.randint(20, 60))]}),
        ("sleep", lambda: {"seconds": random.uniform(0.1, 0.8)}),
        ("matmul", lambda: {
            "A": [[random.randint(1,9) for _ in range(2)] for _ in range(2)],
            "B": [[random.randint(1,9) for _ in range(2)] for _ in range(2)]
        }),
        ("factorial", lambda: {"n": random.randint(6, 12)}),
        ("fibonacci", lambda: {"n": random.randint(10, 20)}),
        ("sum", lambda: {"numbers": [random.randint(1, 100) for _ in range(random.randint(50, 150))]}),
        ("reverse", lambda: {"text": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(20,60)))}),
        ("isprime", lambda: {"n": random.randint(10000, 20000)}),
        ("count_vowels", lambda: {"text": "".join(random.choices("aeiou" + "bcdfgh", k=random.randint(20,80)))}),
        ("gcd", lambda: {"a": random.randint(100,10000), "b": random.randint(100,10000)})
    ]
    typ, gen = random.choice(TYPES)
    payload = gen()
    return typ, payload

# wrapper so run_task is callable by ProcessPool
def task_runner(item):
    # item is (index, type, payload, consider_network_delay)
    idx, typ, payload, net_delay = item
    start = time.time()
    # simulate network overhead (optional) before execution
    if net_delay and net_delay > 0:
        time.sleep(net_delay)
    result = run_task(typ, payload)
    end = time.time()
    return {
        "index": idx,
        "type": typ,
        "start": start,
        "end": end,
        "duration": end - start,
        "result_repr": str(type(result))
    }

# -------------------- experiment functions --------------------
def run_sequential(tasks):
    """Run tasks sequentially in-process. Return per-task records and total wall time."""
    records = []
    t0 = time.time()
    for i, (typ, payload) in enumerate(tasks):
        s = time.time()
        res = run_task(typ, payload)
        e = time.time()
        records.append({"index": i, "type": typ, "start": s, "end": e, "duration": e - s})
    t1 = time.time()
    return records, t1 - t0

def run_parallel(tasks, n_workers=4, net_delay=0.0):
    """Run tasks using ProcessPoolExecutor simulating n_workers worker nodes.
       net_delay adds simulated network overhead (seconds) per task.
    """
    records = []
    t0 = time.time()
    # prepare items for the pool
    items = [(i, typ, payload, net_delay) for i, (typ, payload) in enumerate(tasks)]
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(task_runner, it): it[0] for it in items}
        for fut in as_completed(futures):
            rec = fut.result()
            records.append(rec)
    t1 = time.time()
    return records, t1 - t0

# -------------------- aggregator --------------------
def aggregate(records, makespan):
    per_type = defaultdict(list)
    durations = []
    for r in records:
        d = r["duration"]
        durations.append(d)
        per_type[r["type"]].append(d)
    agg = {
        "makespan": makespan,
        "n_tasks": len(records),
        "avg_latency": mean(durations) if durations else 0.0,
        "throughput": (len(records) / makespan) if makespan > 0 else 0.0,
        "per_type_avg": {k: mean(v) for k, v in per_type.items()}
    }
    return agg

# -------------------- main experiment runner --------------------
def run_experiments(num_tasks=200, repeats=3, worker_list=[1,2,4,8], net_delay=0.0, out_csv="results.csv", plot=False):
    # generate workload once and reuse same task set for fair comparison
    workload = [gen_random_task() for _ in range(num_tasks)]
    header = ["scenario", "workers", "repeat", "n_tasks", "makespan", "avg_latency", "throughput"]
    rows = []
    # 1) Sequential baseline (workers=1 sequential)
    for rep in range(1, repeats+1):
        recs, makespan = run_sequential(workload)
        agg = aggregate(recs, makespan)
        rows.append(["sequential", 1, rep, agg["n_tasks"], agg["makespan"], agg["avg_latency"], agg["throughput"]])
        print(f"[sequential] rep={rep} tasks={agg['n_tasks']} makespan={agg['makespan']:.3f}s throughput={agg['throughput']:.3f} t/s")
    # 2) Parallel experiments
    for w in worker_list:
        for rep in range(1, repeats+1):
            recs, makespan = run_parallel(workload, n_workers=w, net_delay=net_delay)
            agg = aggregate(recs, makespan)
            rows.append(["parallel", w, rep, agg["n_tasks"], agg["makespan"], agg["avg_latency"], agg["throughput"]])
            print(f"[parallel] workers={w} rep={rep} makespan={agg['makespan']:.3f}s throughput={agg['throughput']:.3f} t/s")
    # save CSV
    with open(out_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)
    print(f"\nSaved results to {out_csv}")
    # optional plotting
    if plot:
        try:
            import matplotlib.pyplot as plt
            # aggregate by worker count
            from collections import defaultdict
            stats = defaultdict(list)
            for r in rows:
                scenario, w, rep, n_tasks, makespan, avg_latency, throughput = r[0], int(r[1]), int(r[2]), int(r[3]), float(r[4]), float(r[5]), float(r[6])
                stats[w].append((makespan, avg_latency, throughput))
            ws = sorted(stats.keys())
            mean_makespans = [mean([x[0] for x in stats[w]]) for w in ws]
            mean_throughputs = [mean([x[2] for x in stats[w]]) for w in ws]
            mean_latencies = [mean([x[1] for x in stats[w]]) for w in ws]
            # make plots
            plt.figure(figsize=(8,5))
            plt.plot(ws, mean_makespans, marker='o')
            plt.title("Makespan vs #workers")
            plt.xlabel("Workers")
            plt.ylabel("Makespan (s)")
            plt.grid(True)
            plt.savefig("makespan_vs_workers.png")
            plt.close()
            plt.figure(figsize=(8,5))
            plt.plot(ws, mean_throughputs, marker='o')
            plt.title("Throughput vs #workers")
            plt.xlabel("Workers")
            plt.ylabel("Throughput (tasks/sec)")
            plt.grid(True)
            plt.savefig("throughput_vs_workers.png")
            plt.close()
            plt.figure(figsize=(8,5))
            plt.plot(ws, mean_latencies, marker='o')
            plt.title("Average Task Latency vs #workers")
            plt.xlabel("Workers")
            plt.ylabel("Avg latency (s)")
            plt.grid(True)
            plt.savefig("latency_vs_workers.png")
            plt.close()
            print("Plots saved: makespan_vs_workers.png, throughput_vs_workers.png, latency_vs_workers.png")
        except Exception as e:
            print("Plotting failed:", e)

# -------------------- CLI --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=200, help="Number of tasks in workload")
    parser.add_argument("--repeat", type=int, default=3, help="Repeats per scenario")
    parser.add_argument("--workers", nargs="+", type=int, default=[1,2,4,8], help="Worker counts to test")
    parser.add_argument("--net-delay", type=float, default=0.0, help="Simulated network delay (s) per task")
    parser.add_argument("--out", type=str, default="results.csv", help="Output CSV filename")
    parser.add_argument("--plot", action="store_true", help="Generate plots (matplotlib required)")
    args = parser.parse_args()
    run_experiments(num_tasks=args.tasks, repeats=args.repeat, worker_list=args.workers, net_delay=args.net_delay, out_csv=args.out, plot=args.plot)
