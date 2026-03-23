from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import time
import datetime
import requests
import os

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
        "messages_stored": len(messages)
    }


@app.post("/send")
def send_message(msg: Message):

    message = {
        "id": str(uuid.uuid4()),
        "sender": msg.sender,
        "receiver": msg.receiver,
        "content": msg.content,
        "timestamp": datetime.datetime.now().astimezone().isoformat()
    }

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
    # avoid duplicates
    if not any(m["id"] == message["id"] for m in messages):
        messages.append(message)

    return {"replicated_at": NODE_ID}


@app.get("/messages")
def get_messages():
    return {
        "node": NODE_ID,
        "messages": messages
    }
