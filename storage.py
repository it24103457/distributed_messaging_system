import json
import os
import state

def merge_vector_clocks(vc1, vc2):
    res = {}
    for k in set(vc1.keys()).union(vc2.keys()):
        res[k] = max(vc1.get(k, 0), vc2.get(k, 0))
    return res

def is_newer(clock1, clock2, timestamp1, timestamp2):
    v1_greater_or_equal = True
    v2_greater_or_equal = True
    keys = set(clock1.keys()).union(clock2.keys())
    for k in keys:
        v1_val = clock1.get(k, 0)
        v2_val = clock2.get(k, 0)
        if v1_val < v2_val:
            v1_greater_or_equal = False
        if v2_val < v1_val:
            v2_greater_or_equal = False
            
    if v1_greater_or_equal and not v2_greater_or_equal:
        return True
    if v2_greater_or_equal and not v1_greater_or_equal:
        return False
        
    return timestamp1 > timestamp2

import threading

wal_lock = threading.Lock()

def compact_wal():
    with state.vector_clock_lock:
        vc_copy = state.vector_clock.copy()

    snapshot = {
        "messages": state.messages,
        "vector_clock": vc_copy
    }
    tmp_snap = state.SNAPSHOT_FILE + ".tmp"
    with open(tmp_snap, "w") as f:
        json.dump(snapshot, f)
    os.replace(tmp_snap, state.SNAPSHOT_FILE)
    
    open(state.WAL_FILE, "w").close()
    
    state.wal_count = 0
    import config
    print(f"[{config.NODE_ID}] WAL Compacted. Snapshot saved.", flush=True)

def append_message(message):
    with wal_lock:
        found = False
        update_needed = True
        
        for i, m in enumerate(state.messages):
            if m["id"] == message["id"]:
                found = True
                if not is_newer(message.get("clock", {}), m.get("clock", {}), message.get("timestamp", ""), m.get("timestamp", "")):
                    update_needed = False
                else:
                    state.messages[i] = message
                break
                
        if not found:
            state.messages.append(message)

        if not update_needed:
            return

        with open(state.WAL_FILE, "a") as f:
            f.write(json.dumps(message) + "\n")
            
        state.wal_count += 1
        if state.wal_count >= state.COMPACTION_THRESHOLD:
            compact_wal()

def get_message_by_id(msg_id):
    with wal_lock:
        for m in state.messages:
            if m["id"] == msg_id:
                return m.copy()
    return None

def init_storage():
    if os.path.exists(state.SNAPSHOT_FILE):
        with open(state.SNAPSHOT_FILE, "r") as f:
            data = json.load(f)
            state.messages = data.get("messages", [])
            with state.vector_clock_lock:
                state.vector_clock = data.get("vector_clock", {})

    if os.path.exists(state.WAL_FILE):
        with open(state.WAL_FILE, "r") as f:
            for line in f:
                if line.strip():
                    msg = json.loads(line)
                    found = False
                    for i, m in enumerate(state.messages):
                        if m["id"] == msg["id"]:
                            state.messages[i] = msg
                            found = True
                            break
                    if not found:
                        state.messages.append(msg)
                    
                    
                    with state.vector_clock_lock:
                        state.vector_clock = merge_vector_clocks(state.vector_clock, msg.get("clock", {}))
                    state.wal_count += 1

    if state.wal_count >= state.COMPACTION_THRESHOLD:
        compact_wal()

def merge_messages(incoming_msgs):
    with wal_lock:
        local_map = {m["id"]: m for m in state.messages}
        updated_msgs = []

        for m in incoming_msgs:
            existing = local_map.get(m["id"])

            if not existing or is_newer(m.get("clock", {}), existing.get("clock", {}), m.get("timestamp", ""), existing.get("timestamp", "")):
                local_map[m["id"]] = m
                updated_msgs.append(m)

            with state.vector_clock_lock:
                state.vector_clock = merge_vector_clocks(state.vector_clock, m.get("clock", {}))

        if not updated_msgs:
            return

        state.messages = list(local_map.values())

        with open(state.WAL_FILE, "a") as f:
            for m in updated_msgs:
                f.write(json.dumps(m) + "\n")

        state.wal_count += len(updated_msgs)
        if state.wal_count >= state.COMPACTION_THRESHOLD:
            compact_wal()
