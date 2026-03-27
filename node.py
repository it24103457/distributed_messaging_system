from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import time
import datetime
import requests
import os
import json

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

# -------- MESSAGE MODEL --------

class Message(BaseModel):
    sender: str
    receiver: str
    content: str


# -------- ENDPOINTS --------

@app.get("/")
def status():
    return {
        "node": NODE_ID,
        "port": PORT,
        "clock": lamport_clock,
        "messages_stored": len(messages)
    }


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


@app.post("/replicate")
def replicate_message(message: dict):
    global lamport_clock
    # update local clock based on incoming message clock
    lamport_clock = max(lamport_clock, message.get("clock", 0)) + 1
    
    # avoid duplicates
    if not any(m["id"] == message["id"] for m in messages):
        append_to_wal(message)
        messages.append(message)

    return {"replicated_at": NODE_ID}


@app.get("/messages")
def get_messages():
    # Sort primarily by Lamport clock, tie-break by message ID for deterministic total ordering
    sorted_messages = sorted(messages, key=lambda m: (m.get("clock", 0), m.get("id", "")))
    return {
        "node": NODE_ID,
        "messages": sorted_messages
    }
