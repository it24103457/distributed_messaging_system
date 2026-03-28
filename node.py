from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import time
import datetime
import requests
import os
import json
import threading

app = FastAPI()

# -------- CONFIG --------

NODE_ID = os.getenv("NODE_ID", "Node")
PORT = int(os.getenv("PORT", 5001))

PEERS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003"
]

# 🔥 FIXED: TOTAL CLUSTER SIZE
TOTAL_NODES = len(PEERS)
QUORUM = (TOTAL_NODES // 2) + 1

# -------- STORAGE --------

messages = []
lamport_clock = 0
WAL_FILE = f"wal_{PORT}.jsonl"

def append_to_wal(message):
    with open(WAL_FILE, "a") as f:
        f.write(json.dumps(message) + "\n")

# Load WAL
if os.path.exists(WAL_FILE):
    with open(WAL_FILE, "r") as f:
        for line in f:
            if line.strip():
                msg = json.loads(line)
                messages.append(msg)
                lamport_clock = max(lamport_clock, msg.get("clock", 0))

# -------- STATE --------

active_peers = set()
current_leader = f"http://localhost:{PORT}"

# -------- HEARTBEAT --------

def heartbeat_task():
    global current_leader

    while True:
        current_active = set()

        for peer in PEERS:
            if f":{PORT}" not in peer:
                try:
                    res = requests.get(peer + "/ping", timeout=3)
                    if res.status_code == 200:
                        current_active.add(peer)
                except:
                    pass

        active_peers.clear()
        active_peers.update(current_active)

        # Leader election (highest URL wins)
        all_alive = list(active_peers) + [f"http://localhost:{PORT}"]
        current_leader = max(all_alive)

        time.sleep(5)

@app.on_event("startup")
def startup():
    threading.Thread(target=heartbeat_task, daemon=True).start()

# -------- MODELS --------

class Message(BaseModel):
    sender: str
    receiver: str
    content: str

class EditMessage(BaseModel):
    id: str
    content: str

# -------- UTILS --------

# 🔥 FIXED: Quorum based on TOTAL nodes
def quorum_write(message):
    success = 1  # self always counts

    for peer in PEERS:
        if f":{PORT}" in peer:
            continue

        try:
            res = requests.post(peer + "/replicate", json=message, timeout=1)
            if res.status_code == 200:
                success += 1
        except:
            pass

    return success >= QUORUM


# 🔥 FIXED: Quorum read based on TOTAL nodes
def quorum_read():
    responses = []

    # include self
    responses.append(messages)

    for peer in PEERS:
        if f":{PORT}" in peer:
            continue

        try:
            res = requests.get(peer + "/messages_local", timeout=1)
            if res.status_code == 200:
                responses.append(res.json().get("messages", []))
        except:
            pass

    if len(responses) < QUORUM:
        return None

    # merge with LWW
    merged = {}

    for node_msgs in responses:
        for m in node_msgs:
            existing = merged.get(m["id"])
            if not existing or m["clock"] > existing["clock"]:
                merged[m["id"]] = m

    return list(merged.values())

# -------- ENDPOINTS --------

@app.get("/")
def status():
    return {
        "node": NODE_ID,
        "port": PORT,
        "clock": lamport_clock,
        "messages": len(messages),
        "active_peers": list(active_peers),
        "leader": current_leader,
        "quorum_required": QUORUM
    }

@app.get("/ping")
def ping():
    return {"status": "alive"}

@app.get("/messages_local")
def local_messages():
    return {"messages": messages}

# -------- SEND --------

@app.post("/send")
def send(msg: Message):

    # Forward to leader
    if current_leader != f"http://localhost:{PORT}":
        try:
            return requests.post(current_leader + "/send", json=msg.dict(), timeout=10).json()
        except:
            return {"error": "Leader unavailable"}

    global lamport_clock
    lamport_clock += 1

    message = {
        "id": str(uuid.uuid4()),
        "sender": msg.sender,
        "receiver": msg.receiver,
        "content": msg.content,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "clock": lamport_clock
    }

    # 🔥 QUORUM FIRST
    if not quorum_write(message):
        return {
            "error": "Quorum not achieved",
            "required": QUORUM,
            "hint": "At least 2 nodes must be alive"
        }

    # Commit only after quorum
    append_to_wal(message)
    messages.append(message)

    return {"stored_at": NODE_ID, "message": message}

# -------- EDIT --------

@app.put("/edit")
def edit(edit_msg: EditMessage):

    if current_leader != f"http://localhost:{PORT}":
        try:
            return requests.put(current_leader + "/edit", json=edit_msg.dict(), timeout=10).json()
        except:
            return {"error": "Leader unavailable"}

    global lamport_clock
    lamport_clock += 1

    for i, m in enumerate(messages):
        if m["id"] == edit_msg.id:

            updated = m.copy()
            updated["content"] = edit_msg.content
            updated["clock"] = lamport_clock
            updated["timestamp"] = datetime.datetime.now().astimezone().isoformat()

            # 🔥 QUORUM FIRST
            if not quorum_write(updated):
                return {"error": "Quorum not achieved"}

            messages[i] = updated
            append_to_wal(updated)

            return {"edited_at": NODE_ID, "message": updated}

    return {"error": "Message not found"}

# -------- REPLICATION --------

@app.post("/replicate")
def replicate(message: dict):
    global lamport_clock

    lamport_clock = max(lamport_clock, message.get("clock", 0)) + 1

    # Deduplication + LWW
    for i, m in enumerate(messages):
        if m["id"] == message["id"]:
            if message["clock"] > m["clock"]:
                messages[i] = message
                append_to_wal(message)
            return {"status": "updated"}

    messages.append(message)
    append_to_wal(message)

    return {"status": "added"}

# -------- READ --------

@app.get("/messages")
def get_messages():

    result = quorum_read()

    if result is None:
        return {
            "error": "Quorum read failed",
            "required": QUORUM
        }

    sorted_msgs = sorted(result, key=lambda m: (m["clock"], m["id"]))

    return {
        "node": NODE_ID,
        "messages": sorted_msgs
    }