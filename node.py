from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uuid
import time
import datetime
import requests
import os
import json
import threading
import random

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# -------- CONFIG --------

NODE_ID = os.getenv("NODE_ID", "Node")
PORT = int(os.getenv("PORT", 5001))

SELF_URL = f"http://localhost:{PORT}"

PEERS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003",
    "http://localhost:5004",
    "http://localhost:5005"
]

TOTAL_NODES = len(PEERS)
QUORUM = (TOTAL_NODES // 2) + 1

# -------- STORAGE --------

messages = []
vector_clock = {peer: 0 for peer in PEERS}
WAL_FILE = f"wal_{PORT}.jsonl"

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
    with open(WAL_FILE, "a") as f:
        f.write(json.dumps(message) + "\n")

if os.path.exists(WAL_FILE):
    with open(WAL_FILE, "r") as f:
        for line in f:
            if line.strip():
                msg = json.loads(line)
                messages.append(msg)
                vector_clock = merge_vector_clocks(vector_clock, msg.get("clock", {}))

# -------- STATE --------

active_peers = set()
current_leader = None

node_state = "FOLLOWER"
current_term = 0
voted_for = None
last_heartbeat = time.time()

# -------- LEADER ELECTION (RAFT) --------

def reset_election_timeout():
    global last_heartbeat
    last_heartbeat = time.time()

def get_election_timeout():
    return random.uniform(3.0, 6.0)

def election_timer_task():
    global node_state, current_term, voted_for, current_leader
    
    timeout = get_election_timeout()
    
    while True:
        time.sleep(0.1)
        
        if node_state == "LEADER":
            continue
            
        if time.time() - last_heartbeat > timeout:
            print(f"[{NODE_ID}] Election timeout! Starting election for term {current_term + 1}", flush=True)
            node_state = "CANDIDATE"
            current_term += 1
            voted_for = SELF_URL
            votes_received = 1
            reset_election_timeout()
            timeout = get_election_timeout()
            
            for peer in PEERS:
                if peer == SELF_URL:
                    continue
                try:
                    res = requests.post(peer + "/request_vote", json={
                        "term": current_term,
                        "candidate_id": SELF_URL
                    }, timeout=1)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("term", 0) > current_term:
                            print(f"[{NODE_ID}] Stepping down. {peer} has term {data.get('term')} > my {current_term}", flush=True)
                            current_term = data.get("term")
                            node_state = "FOLLOWER"
                            voted_for = None
                            reset_election_timeout()
                            break
                        if data.get("vote_granted"):
                            print(f"[{NODE_ID}] Got vote from {peer}", flush=True)
                            votes_received += 1
                        else:
                            print(f"[{NODE_ID}] Vote rejected by {peer}. Data: {data}", flush=True)
                except Exception as e:
                    print(f"[{NODE_ID}] Error requesting vote from {peer}: {type(e).__name__}", flush=True)
            
            print(f"[{NODE_ID}] Election for term {current_term} ended. Votes: {votes_received}/{QUORUM}, state={node_state}", flush=True)
            if node_state == "CANDIDATE" and votes_received >= QUORUM:
                print(f"[{NODE_ID}] Won election! I am the new leader for term {current_term}", flush=True)
                node_state = "LEADER"
                current_leader = SELF_URL
                
                # Immediately send heartbeats
                send_heartbeats()

def send_heartbeats():
    global node_state, current_term, voted_for
    
    alive = []
    
    for peer in PEERS:
        if peer == SELF_URL:
            continue
        try:
            res = requests.post(peer + "/append_entries", json={
                "term": current_term,
                "leader_id": SELF_URL
            }, timeout=1)
            if res.status_code == 200:
                data = res.json()
                if data.get("term", 0) > current_term:
                    current_term = data.get("term")
                    node_state = "FOLLOWER"
                    voted_for = None
                    return alive
                elif data.get("success"):
                    alive.append(peer)
        except:
            pass
            
    return alive

def heartbeat_task():
    global active_peers
    
    while True:
        if node_state == "LEADER":
            alive = send_heartbeats()
            active_peers.clear()
            active_peers.update(alive)
            
        time.sleep(1)

# -------- RECOVERY --------

def merge_messages(incoming_msgs):
    global messages, vector_clock

    local_map = {m["id"]: m for m in messages}

    for m in incoming_msgs:
        existing = local_map.get(m["id"])

        if not existing or is_newer(m.get("clock", {}), existing.get("clock", {}), m.get("timestamp", ""), existing.get("timestamp", "")):
            local_map[m["id"]] = m

        vector_clock = merge_vector_clocks(vector_clock, m.get("clock", {}))

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
    def delayed_recovery():
        time.sleep(2)
        recover_node()

    threading.Thread(target=heartbeat_task, daemon=True).start()
    threading.Thread(target=election_timer_task, daemon=True).start()
    threading.Thread(target=delayed_recovery, daemon=True).start()
    threading.Thread(target=periodic_sync, daemon=True).start()

# -------- MODELS --------

class Message(BaseModel):
    sender: str
    receiver: str
    content: str

class EditMessage(BaseModel):
    id: str
    content: str

class RequestVote(BaseModel):
    term: int
    candidate_id: str

class AppendEntries(BaseModel):
    term: int
    leader_id: str

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
            if not existing or is_newer(m.get("clock", {}), existing.get("clock", {}), m.get("timestamp", ""), existing.get("timestamp", "")):
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

@app.post("/request_vote")
def request_vote(req: RequestVote):
    global current_term, voted_for, node_state
    
    if req.term > current_term:
        current_term = req.term
        node_state = "FOLLOWER"
        voted_for = None
        
    vote_granted = False
    
    if req.term >= current_term and (voted_for is None or voted_for == req.candidate_id):
        voted_for = req.candidate_id
        vote_granted = True
        reset_election_timeout()
        
    return {"term": current_term, "vote_granted": vote_granted}

@app.post("/append_entries")
def append_entries(req: AppendEntries):
    global current_term, node_state, current_leader, voted_for
    
    if req.term >= current_term:
        current_term = req.term
        node_state = "FOLLOWER"
        current_leader = req.leader_id
        reset_election_timeout()
        return {"term": current_term, "success": True}
        
    return {"term": current_term, "success": False}

# -------- SEND --------

@app.post("/send")
def send(msg: Message):

    if current_leader != SELF_URL:
        if current_leader is None:
            return {"error": "No leader elected yet. Please retry later."}
        try:
            return requests.post(current_leader + "/send", json=msg.dict(), timeout=10).json()
        except Exception as e:
            print(f"[{NODE_ID}] Error forwarding to leader: {e}")
            return {"error": "Leader unavailable or election in progress. Please retry later."}

    global vector_clock
    vector_clock[SELF_URL] = vector_clock.get(SELF_URL, 0) + 1

    message = {
        "id": str(uuid.uuid4()),
        "sender": msg.sender,
        "receiver": msg.receiver,
        "content": msg.content,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "clock": vector_clock.copy()
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
        if current_leader is None:
            return {"error": "No leader elected yet. Please retry later."}
        try:
            return requests.put(current_leader + "/edit", json=edit_msg.dict(), timeout=10).json()
        except Exception as e:
            print(f"[{NODE_ID}] Error forwarding edit to leader: {e}")
            return {"error": "Leader unavailable or election in progress. Please retry later."}

    global vector_clock
    vector_clock[SELF_URL] = vector_clock.get(SELF_URL, 0) + 1

    for i, m in enumerate(messages):
        if m["id"] == edit_msg.id:

            updated = m.copy()
            updated["content"] = edit_msg.content
            updated["clock"] = vector_clock.copy()
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
    global vector_clock

    vector_clock = merge_vector_clocks(vector_clock, message.get("clock", {}))

    for i, m in enumerate(messages):
        if m["id"] == message["id"]:
            if is_newer(message.get("clock", {}), m.get("clock", {}), message.get("timestamp", ""), m.get("timestamp", "")):
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

    sorted_msgs = sorted(result, key=lambda m: (sum(m.get("clock", {}).values()), m["id"]))

    return {"messages": sorted_msgs}