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

SELF_URL = f"http://localhost:{PORT}"

PEERS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003"
]

TOTAL_NODES = len(PEERS)
QUORUM = (TOTAL_NODES // 2) + 1

# -------- STORAGE --------

messages = []
lamport_clock = 0
WAL_FILE = f"wal_{PORT}.jsonl"

def append_to_wal(message):
    with open(WAL_FILE, "a") as f:
        f.write(json.dumps(message) + "\n")

if os.path.exists(WAL_FILE):
    with open(WAL_FILE, "r") as f:
        for line in f:
            if line.strip():
                msg = json.loads(line)
                messages.append(msg)
                lamport_clock = max(lamport_clock, msg.get("clock", 0))

# -------- STATE --------

active_peers = set()
current_leader = SELF_URL

# -------- HEARTBEAT --------

def heartbeat_task():
    global current_leader

    while True:
        alive = []

        for peer in PEERS:
            try:
                res = requests.get(peer + "/ping", timeout=1)
                if res.status_code == 200:
                    alive.append(peer)
            except:
                pass

        active_peers.clear()
        active_peers.update([p for p in alive if p != SELF_URL])

        if alive:
            current_leader = max(alive)

        time.sleep(5)

# -------- LEADER ELECTION --------

def elect_new_leader():
    global current_leader

    alive = [SELF_URL]

    for peer in PEERS:
        if peer == SELF_URL:
            continue
        try:
            res = requests.get(peer + "/ping", timeout=1)
            if res.status_code == 200:
                alive.append(peer)
        except:
            pass

    if not alive:
        current_leader = SELF_URL
    else:
        current_leader = max(alive)

    print(f"[{NODE_ID}] New leader: {current_leader}")

# -------- RECOVERY --------

def merge_messages(incoming_msgs):
    global messages, lamport_clock

    local_map = {m["id"]: m for m in messages}

    for m in incoming_msgs:
        existing = local_map.get(m["id"])

        if not existing or m["clock"] > existing["clock"]:
            local_map[m["id"]] = m

        lamport_clock = max(lamport_clock, m.get("clock", 0))

    messages = list(local_map.values())

    with open(WAL_FILE, "w") as f:
        for m in messages:
            f.write(json.dumps(m) + "\n")

def recover_node():
    print(f"[{NODE_ID}] Recovery...")

    for peer in PEERS:
        if peer == SELF_URL:
            continue

        try:
            res = requests.get(peer + "/sync", timeout=2)
            if res.status_code == 200:
                merge_messages(res.json().get("messages", []))
                print(f"[{NODE_ID}] Synced from {peer}")
                return
        except:
            pass

def periodic_sync():
    while True:
        time.sleep(10)
        recover_node()

@app.on_event("startup")
def startup():
    threading.Thread(target=heartbeat_task, daemon=True).start()

    time.sleep(2)
    recover_node()

    threading.Thread(target=periodic_sync, daemon=True).start()

# -------- MODELS --------

class Message(BaseModel):
    sender: str
    receiver: str
    content: str

class EditMessage(BaseModel):
    id: str
    content: str

# -------- UTILS --------

def quorum_write(message):
    success = 1

    for peer in PEERS:
        if peer == SELF_URL:
            continue

        try:
            res = requests.post(peer + "/replicate", json=message, timeout=1)
            if res.status_code == 200:
                success += 1
        except:
            pass

    return success >= QUORUM

def quorum_read():
    responses = [messages]

    for peer in PEERS:
        if peer == SELF_URL:
            continue

        try:
            res = requests.get(peer + "/messages_local", timeout=1)
            if res.status_code == 200:
                responses.append(res.json().get("messages", []))
        except:
            pass

    if len(responses) < QUORUM:
        return None

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
        "leader": current_leader,
        "active_peers": list(active_peers),
        "messages": len(messages),
        "quorum": QUORUM
    }

@app.get("/ping")
def ping():
    return {"status": "alive"}

@app.get("/messages_local")
def local_messages():
    return {"messages": messages}

@app.get("/sync")
def sync():
    return {"messages": messages}

# -------- SEND --------

@app.post("/send")
def send(msg: Message):

    if current_leader != SELF_URL:
        try:
            return requests.post(current_leader + "/send", json=msg.dict(), timeout=3).json()
        except:
            print(f"[{NODE_ID}] Leader failed → electing")

            elect_new_leader()

            if current_leader != SELF_URL:
                try:
                    return requests.post(current_leader + "/send", json=msg.dict(), timeout=10).json()
                except:
                    return {"error": "Leader election failed"}

            print(f"[{NODE_ID}] I am new leader → processing locally")

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

    if not quorum_write(message):
        return {"error": "Quorum not achieved"}

    append_to_wal(message)
    messages.append(message)

    return {"stored_at": NODE_ID, "message": message}

# -------- EDIT --------

@app.put("/edit")
def edit(edit_msg: EditMessage):

    if current_leader != SELF_URL:
        try:
            return requests.put(current_leader + "/edit", json=edit_msg.dict(), timeout=3).json()
        except:
            print(f"[{NODE_ID}] Leader failed → electing")

            elect_new_leader()

            if current_leader != SELF_URL:
                try:
                    return requests.put(current_leader + "/edit", json=edit_msg.dict(), timeout=10).json()
                except:
                    return {"error": "Leader election failed"}

            print(f"[{NODE_ID}] I am new leader → processing locally")

    global lamport_clock
    lamport_clock += 1

    for i, m in enumerate(messages):
        if m["id"] == edit_msg.id:

            updated = m.copy()
            updated["content"] = edit_msg.content
            updated["clock"] = lamport_clock
            updated["timestamp"] = datetime.datetime.now().astimezone().isoformat()

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
        return {"error": "Quorum read failed"}

    sorted_msgs = sorted(result, key=lambda m: (m["clock"], m["id"]))

    return {"messages": sorted_msgs}