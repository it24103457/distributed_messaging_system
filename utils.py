import requests
import time
import concurrent.futures
import config
import state
import storage

def quorum_write(message):
    success = 1

    def send_replica(peer):
        try:
            res = requests.post(peer + "/replicate", json=message, timeout=1)
            return res.status_code == 200
        except:
            return False

    peers_to_sync = [p for p in config.PEERS if p != config.SELF_URL]
    
    if not peers_to_sync:
        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(peers_to_sync))) as executor:
        results = executor.map(send_replica, peers_to_sync)
        success += sum(1 for r in results if r)

    return success >= config.QUORUM

def quorum_read():
    responses = [state.messages]

    def fetch_msgs(peer):
        try:
            res = requests.get(peer + "/messages_local", timeout=1)
            if res.status_code == 200:
                return res.json().get("messages", [])
        except:
            return None
        return None

    peers_to_sync = [p for p in config.PEERS if p != config.SELF_URL]
    
    if not peers_to_sync:
        return responses

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(peers_to_sync))) as executor:
        results = executor.map(fetch_msgs, peers_to_sync)
        for r in results:
            if r is not None:
                responses.append(r)

    if len(responses) < config.QUORUM:
        return None

    merged = {}

    for node_msgs in responses:
        for m in node_msgs:
            existing = merged.get(m["id"])
            if not existing or storage.is_newer(m.get("clock", {}), existing.get("clock", {}), m.get("timestamp", ""), existing.get("timestamp", "")):
                merged[m["id"]] = m

    return list(merged.values())

def recover_node():
    print(f"[{config.NODE_ID}] Recovery...")

    for peer in config.PEERS:
        if peer == config.SELF_URL:
            continue

        try:
            res = requests.get(peer + "/sync", timeout=2)
            if res.status_code == 200:
                storage.merge_messages(res.json().get("messages", []))
                print(f"[{config.NODE_ID}] Synced from {peer}")
                return
        except:
            pass

def periodic_sync():
    while True:
        time.sleep(10)
        recover_node()
