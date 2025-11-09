# submit_tasks.py
import requests
import random
import time
import string

MASTER = "http://127.0.0.1:5000"

# ---------- Random Payload Generators ----------
def random_sort():
    return {"type": "sort", "payload": {"array": [random.randint(1, 100) for _ in range(random.randint(5, 10))]}, "priority": random.randint(20, 60)}

def random_sleep():
    return {"type": "sleep", "payload": {"seconds": random.randint(2, 5)}, "priority": random.randint(40, 70)}

def random_matmul():
    size = random.randint(2, 3)
    A = [[random.randint(1, 9) for _ in range(size)] for _ in range(size)]
    B = [[random.randint(1, 9) for _ in range(size)] for _ in range(size)]
    return {"type": "matmul", "payload": {"A": A, "B": B}, "priority": random.randint(30, 50)}

def random_sum():
    return {"type": "sum", "payload": {"numbers": [random.randint(1, 100) for _ in range(random.randint(5, 15))]}, "priority": random.randint(25, 65)}

def random_factorial():
    return {"type": "factorial", "payload": {"n": random.randint(3, 10)}, "priority": random.randint(30, 70)}

def random_fibonacci():
    return {"type": "fibonacci", "payload": {"n": random.randint(5, 15)}, "priority": random.randint(20, 60)}

def random_reverse():
    txt = ''.join(random.choices(string.ascii_letters, k=random.randint(5, 10)))
    return {"type": "reverse", "payload": {"text": txt}, "priority": random.randint(20, 70)}

def random_isprime():
    return {"type": "isprime", "payload": {"n": random.randint(10, 100)}, "priority": random.randint(25, 60)}

def random_count_vowels():
    txt = ''.join(random.choices(string.ascii_lowercase, k=random.randint(8, 15)))
    return {"type": "count_vowels", "payload": {"text": txt}, "priority": random.randint(25, 70)}

def random_gcd():
    return {"type": "gcd", "payload": {"a": random.randint(10, 100), "b": random.randint(10, 100)}, "priority": random.randint(20, 70)}

# ---------- Task Registry ----------
GENS = [
    random_sort, random_sleep, random_matmul,
    random_sum, random_factorial, random_fibonacci,
    random_reverse, random_isprime, random_count_vowels, random_gcd
]

# ---------- Continuous Random Task Generator ----------
def submit_random_tasks(n=0, delay_range=(1, 3)):
    """
    n = 0 → run forever
    otherwise submit exactly n random tasks
    """
    i = 0
    print(f"🚀 Submitting random tasks to {MASTER}")
    print("Press Ctrl + C to stop.\n")
    try:
        while n == 0 or i < n:
            i += 1
            task = random.choice(GENS)()
            try:
                r = requests.post(f"{MASTER}/submit", json=task, timeout=5)
                if r.status_code == 201:
                    print(f"[{i}] Sent {task['type']} (prio={task['priority']})")
                else:
                    print(f"[!] Master returned {r.status_code}: {r.text}")
            except requests.exceptions.RequestException as e:
                print(f"[!] Failed to send: {e}")
            time.sleep(random.uniform(*delay_range))
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")

if __name__ == "__main__":
    submit_random_tasks(n=0)  # 0 = infinite mode
