import requests
import time
import config
import state
import storage

def quorum_write(message):
    success = 1

    for peer in config.PEERS:
        if peer == config.SELF_URL:
            continue

        try:
            res = requests.post(peer + "/replicate", json=message, timeout=1)
            if res.status_code == 200:
                success += 1
        except:
            pass

    return success >= config.QUORUM

def quorum_read():
    responses = [state.messages]

    for peer in config.PEERS:
        if peer == config.SELF_URL:
            continue

        try:
            res = requests.get(peer + "/messages_local", timeout=1)
            if res.status_code == 200:
                responses.append(res.json().get("messages", []))
        except:
            pass

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
