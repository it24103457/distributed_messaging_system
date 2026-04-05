import requests
import concurrent.futures
import time

URL = "http://localhost:5001/send"
TOTAL_MESSAGES = 500
CONCURRENCY = 100

success_count = 0
error_count = 0
errors = []

def send_message(i):
    global success_count, error_count, errors
    message = {
        "sender": "load_tester",
        "receiver": "cluster",
        "content": f"Bulk message {i}"
    }
    try:
        start_time = time.time()
        # The node.py /send endpoint requires JSON formatted to match the `Message` Pydantic model
        res = requests.post(URL, json=message, timeout=30)
        end_time = time.time()
        
        if res.status_code == 200:
            data = res.json()
            if "error" in data:
                error_count += 1
                error_msg = f"[{i:03d}] Server Error: {data['error']}"
                errors.append(error_msg)
                print(error_msg)
            else:
                success_count += 1
                stored_node = data.get("stored_at", "Routed via leader")
                print(f"[{i:03d}] Success in {end_time - start_time:.2f}s (Stored by: {stored_node})")
        else:
            error_count += 1
            error_msg = f"[{i:03d}] Failed Code {res.status_code}: {res.text}"
            errors.append(error_msg)
            print(error_msg)
    except Exception as e:
        error_count += 1
        error_msg = f"[{i:03d}] Exception: {e}"
        errors.append(error_msg)
        print(error_msg)

if __name__ == "__main__":
    print(f"Sending {TOTAL_MESSAGES} messages to {URL} concurrently using {CONCURRENCY} threads...")
    print("Ensure the cluster is running (via run_nodes.ps1).\n")
    
    start_total = time.time()
    
    # ThreadPoolExecutor bombs the server to effectively test scalability and stability
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_message, i) for i in range(1, TOTAL_MESSAGES + 1)]
        concurrent.futures.wait(futures)
        
    end_total = time.time()
    
    print(f"\n==================================================")
    print(f"Finished sending {TOTAL_MESSAGES} messages in {end_total - start_total:.2f} seconds.")
    print(f"Success messages: {success_count}")
    print(f"Error messages: {error_count}")
    if errors:
        print("\nError details:")
        for error in errors:
            print(f"  {error}")
    print(f"==================================================")
    print("-> Check your project directory to see if 'snapshot_5001.json' was generated!")
    print("-> Because threshold is 50, it should have snapshotted twice, meaning 'wal_5001.jsonl' should only have a few messages left.")
