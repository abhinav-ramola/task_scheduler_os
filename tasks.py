# tasks.py
import time, random, math

# ----------------- Core Task Functions -----------------

def run_sort(payload):
    arr = payload.get("array", [])
    time.sleep(0.1)  # simulate computation
    return sorted(arr)

def run_sleep(payload):
    t = payload.get("seconds", 1)
    time.sleep(t)
    return {"slept": t}

def run_matmul(payload):
    A = payload.get("A", [])
    B = payload.get("B", [])
    if not A or not B:
        raise ValueError("Empty matrices")
    rowsA, colsA, rowsB, colsB = len(A), len(A[0]), len(B), len(B[0])
    if colsA != rowsB:
        raise ValueError("Incompatible matrices")
    C = [[sum(A[i][k] * B[k][j] for k in range(colsA)) for j in range(colsB)] for i in range(rowsA)]
    return C

def run_sum(payload):
    nums = payload.get("numbers", [])
    time.sleep(0.1)
    return sum(nums)

def run_factorial(payload):
    n = payload.get("n", 5)
    time.sleep(0.1)
    fact = 1
    for i in range(2, n+1):
        fact *= i
    return fact

def run_fibonacci(payload):
    n = payload.get("n", 10)
    time.sleep(0.1)
    a, b = 0, 1
    seq = []
    for _ in range(n):
        seq.append(a)
        a, b = b, a + b
    return seq

def run_reverse(payload):
    text = payload.get("text", "")
    time.sleep(0.05)
    return text[::-1]

def run_isprime(payload):
    n = payload.get("n", 7)
    if n < 2:
        return False
    for i in range(2, int(n**0.5)+1):
        if n % i == 0:
            return False
    return True

def run_count_vowels(payload):
    text = payload.get("text", "")
    vowels = "aeiouAEIOU"
    return sum(1 for c in text if c in vowels)

def run_gcd(payload):
    a = payload.get("a", 10)
    b = payload.get("b", 20)
    while b:
        a, b = b, a % b
    return a

# ----------------- Register All Tasks -----------------
TASKS = {
    "sort": run_sort,
    "sleep": run_sleep,
    "matmul": run_matmul,
    "sum": run_sum,
    "factorial": run_factorial,
    "fibonacci": run_fibonacci,
    "reverse": run_reverse,
    "isprime": run_isprime,
    "count_vowels": run_count_vowels,
    "gcd": run_gcd,
}

def run_task(task_type, payload):
    if task_type not in TASKS:
        raise ValueError(f"Unknown task type: {task_type}")
    return TASKS[task_type](payload)
