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

# -------- STATE & HEARTBEATS --------

active_peers = set()

def heartbeat_task():
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
        
        active_peers.clear()
        active_peers.update(current_active)
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
        "active_peers": list(active_peers)
    }

@app.get("/ping")
def ping():
    return {"status": "alive", "node": NODE_ID}


@app.post("/send")
def send_message(msg: Message):
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
    for peer in PEERS:
        if f":{PORT}" not in peer:
            try:
                requests.post(peer + "/replicate", json=message, timeout=1)
            except:
                pass

    return {"stored_at": NODE_ID, "message": message}

@app.put("/edit")
def edit_message(edit_msg: EditMessage):
    global lamport_clock
    lamport_clock += 1
    
    for i, m in enumerate(messages):
        if m["id"] == edit_msg.id:
            # Create an updated copy
            updated_m = m.copy()
            updated_m["content"] = edit_msg.content
            updated_m["clock"] = lamport_clock
            updated_m["timestamp"] = datetime.datetime.now().astimezone().isoformat()
            
            messages[i] = updated_m
            append_to_wal(updated_m)
            
            # Broadcast update
            for peer in PEERS:
                if f":{PORT}" not in peer:
                    try:
                        requests.post(peer + "/replicate", json=updated_m, timeout=1)
                    except:
                        pass
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
            # If incoming message has a strictly higher logical clock, overwrite ours
            if message.get("clock", 0) > m.get("clock", 0):
                messages[i] = message
                append_to_wal(message)
            # If timestamps match exactly (tie), break tie using sender
            elif message.get("clock", 0) == m.get("clock", 0) and message.get("sender", "") > m.get("sender", ""):
                messages[i] = message
                append_to_wal(message)
            return {"replicated_at": NODE_ID, "status": "conflict_resolved"}

    # If message is completely new, append it
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
