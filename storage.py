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

def append_to_wal(message):
    with open(state.WAL_FILE, "a") as f:
        f.write(json.dumps(message) + "\n")

def init_storage():
    if os.path.exists(state.WAL_FILE):
        with open(state.WAL_FILE, "r") as f:
            for line in f:
                if line.strip():
                    msg = json.loads(line)
                    state.messages.append(msg)
                    state.vector_clock = merge_vector_clocks(state.vector_clock, msg.get("clock", {}))

def merge_messages(incoming_msgs):
    local_map = {m["id"]: m for m in state.messages}

    for m in incoming_msgs:
        existing = local_map.get(m["id"])

        if not existing or is_newer(m.get("clock", {}), existing.get("clock", {}), m.get("timestamp", ""), existing.get("timestamp", "")):
            local_map[m["id"]] = m

        state.vector_clock = merge_vector_clocks(state.vector_clock, m.get("clock", {}))

    state.messages = list(local_map.values())

    with open(state.WAL_FILE, "w") as f:
        for m in state.messages:
            f.write(json.dumps(m) + "\n")
