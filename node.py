from fastapi import FastAPI, BackgroundTasks
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
FALLBACK_TIMEOUT = int(os.getenv("FALLBACK_TIMEOUT", 10))

PEERS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003"
]

# -------- STORAGE --------

messages = []
lamport_clock = 0
WAL_FILE = f"wal_{PORT}.jsonl"

def append_to_wal(message):
    with open(WAL_FILE, "a") as f:
        f.write(json.dumps(message) + "\n")

# Load existing WAL on startup
if os.path.exists(WAL_FILE):
    with open(WAL_FILE, "r") as f:
        for line in f:
            if line.strip():
                msg = json.loads(line)
                messages.append(msg)
                lamport_clock = max(lamport_clock, msg.get("clock", 0))

# -------- STATE & LEADER ELECTION --------

active_peers = set()
current_leader = f"http://localhost:{PORT}"

def heartbeat_task():
    global current_leader
    while True:
        current_active = set()
        for peer in PEERS:
            if f":{PORT}" not in peer:
                try:
                    response = requests.get(peer + "/ping", timeout=1)
                    if response.status_code == 200:
                        current_active.add(peer)
                except requests.exceptions.RequestException:
                    pass
        
        # Leader Election: Highest ID (URL string) wins
        # Atomically update active_peers to avoid empty lists during reads
        global active_peers
        active_peers = current_active
        
        all_alive = list(active_peers) + [f"http://localhost:{PORT}"]
        current_leader = max(all_alive)
        
        time.sleep(5)

@app.on_event("startup")
def startup_event():
    threading.Thread(target=heartbeat_task, daemon=True).start()

# -------- MESSAGE MODEL --------

class Message(BaseModel):
    sender: str
    receiver: str
    content: str

class EditMessage(BaseModel):
    id: str
    content: str


# -------- ENDPOINTS --------

@app.get("/")
def status():
    return {
        "node": NODE_ID,
        "port": PORT,
        "clock": lamport_clock,
        "messages_stored": len(messages),
        "active_peers": list(active_peers),
        "leader": current_leader
    }

@app.get("/ping")
async def ping():
    return {"status": "alive", "node": NODE_ID}


def replicate_to_peers(message):
    for peer in PEERS:
        if f":{PORT}" not in peer:
            try:
                requests.post(peer + "/replicate", json=message, timeout=1)
            except:
                pass

@app.post("/send")
def send_message(msg: Message, background_tasks: BackgroundTasks):
    # Leader-only Writes (Write Forwarding)
    if current_leader and current_leader != f"http://localhost:{PORT}":
        try:
            response = requests.post(current_leader + "/send", json=msg.dict(), timeout=FALLBACK_TIMEOUT)
            return response.json()
        except requests.exceptions.RequestException:
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

    append_to_wal(message)
    messages.append(message)

    # replicate to peers
    background_tasks.add_task(replicate_to_peers, message)

    return {"stored_at": NODE_ID, "message": message}

@app.put("/edit")
def edit_message(edit_msg: EditMessage, background_tasks: BackgroundTasks):
    # Leader-only Writes (Write Forwarding)
    if current_leader and current_leader != f"http://localhost:{PORT}":
        try:
            response = requests.put(current_leader + "/edit", json=edit_msg.dict(), timeout=FALLBACK_TIMEOUT)
            return response.json()
        except requests.exceptions.RequestException:
            return {"error": "Leader unavailable"}

    global lamport_clock
    lamport_clock += 1
    
    for i, m in enumerate(messages):
        if m["id"] == edit_msg.id:
            updated_m = m.copy()
            updated_m["content"] = edit_msg.content
            updated_m["clock"] = lamport_clock
            updated_m["timestamp"] = datetime.datetime.now().astimezone().isoformat()
            
            messages[i] = updated_m
            append_to_wal(updated_m)
            
            # Broadcast update
            background_tasks.add_task(replicate_to_peers, updated_m)
            return {"edited_at": NODE_ID, "message": updated_m}
            
    return {"error": "Message not found"}

@app.post("/replicate")
def replicate_message(message: dict):
    global lamport_clock
    # update local clock based on incoming message clock
    lamport_clock = max(lamport_clock, message.get("clock", 0)) + 1
    
    # Conflict Resolution: Last-Write-Wins (LWW) (Eventual Consistency)
    for i, m in enumerate(messages):
        if m["id"] == message["id"]:
            if message.get("clock", 0) > m.get("clock", 0):
                messages[i] = message
                append_to_wal(message)
            elif message.get("clock", 0) == m.get("clock", 0) and message.get("sender", "") > m.get("sender", ""):
                messages[i] = message
                append_to_wal(message)
            return {"replicated_at": NODE_ID, "status": "conflict_resolved"}

    append_to_wal(message)
    messages.append(message)

    return {"replicated_at": NODE_ID, "status": "added"}


@app.get("/messages")
def get_messages():
    # Sort primarily by Lamport clock, tie-break by message ID for deterministic total ordering
    sorted_messages = sorted(messages, key=lambda m: (m.get("clock", 0), m.get("id", "")))
    return {
        "node": NODE_ID,
        "messages": sorted_messages
    }
