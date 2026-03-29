from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uuid
import datetime
import requests
import threading

import config
from models import Message, EditMessage, RequestVote, AppendEntries
import state
import storage
import raft
import utils

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    storage.init_storage()

    def delayed_recovery():
        import time
        time.sleep(2)
        utils.recover_node()

    threading.Thread(target=raft.heartbeat_task, daemon=True).start()
    threading.Thread(target=raft.election_timer_task, daemon=True).start()
    threading.Thread(target=delayed_recovery, daemon=True).start()
    threading.Thread(target=utils.periodic_sync, daemon=True).start()

# -------- ENDPOINTS --------

@app.get("/")
def status():
    return {
        "node": config.NODE_ID,
        "leader": state.current_leader,
        "active_peers": list(state.active_peers),
        "messages": len(state.messages),
        "quorum": config.QUORUM
    }

@app.get("/ping")
def ping():
    return {"status": "alive"}

@app.get("/messages_local")
def local_messages():
    return {"messages": state.messages}

@app.get("/sync")
def sync():
    return {"messages": state.messages}

@app.post("/request_vote")
def request_vote(req: RequestVote):
    if req.term > state.current_term:
        state.current_term = req.term
        state.node_state = "FOLLOWER"
        state.voted_for = None
        
    vote_granted = False
    
    if req.term >= state.current_term and (state.voted_for is None or state.voted_for == req.candidate_id):
        state.voted_for = req.candidate_id
        vote_granted = True
        raft.reset_election_timeout()
        
    return {"term": state.current_term, "vote_granted": vote_granted}

@app.post("/append_entries")
def append_entries(req: AppendEntries):
    if req.term >= state.current_term:
        state.current_term = req.term
        state.node_state = "FOLLOWER"
        state.current_leader = req.leader_id
        raft.reset_election_timeout()
        return {"term": state.current_term, "success": True}
        
    return {"term": state.current_term, "success": False}

# -------- SEND --------

@app.post("/send")
def send(msg: Message):
    if state.current_leader != config.SELF_URL:
        if state.current_leader is None:
            return {"error": "No leader elected yet. Please retry later."}
        try:
            return requests.post(state.current_leader + "/send", json=msg.dict(), timeout=10).json()
        except Exception as e:
            print(f"[{config.NODE_ID}] Error forwarding to leader: {e}")
            return {"error": "Leader unavailable or election in progress. Please retry later."}

    state.vector_clock[config.SELF_URL] = state.vector_clock.get(config.SELF_URL, 0) + 1

    message = {
        "id": str(uuid.uuid4()),
        "sender": msg.sender,
        "receiver": msg.receiver,
        "content": msg.content,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "clock": state.vector_clock.copy()
    }

    if not utils.quorum_write(message):
        return {"error": "Quorum not achieved"}

    storage.append_to_wal(message)
    state.messages.append(message)

    return {"stored_at": config.NODE_ID, "message": message}

# -------- EDIT --------

@app.put("/edit")
def edit(edit_msg: EditMessage):
    if state.current_leader != config.SELF_URL:
        if state.current_leader is None:
            return {"error": "No leader elected yet. Please retry later."}
        try:
            return requests.put(state.current_leader + "/edit", json=edit_msg.dict(), timeout=10).json()
        except Exception as e:
            print(f"[{config.NODE_ID}] Error forwarding edit to leader: {e}")
            return {"error": "Leader unavailable or election in progress. Please retry later."}

    state.vector_clock[config.SELF_URL] = state.vector_clock.get(config.SELF_URL, 0) + 1

    for i, m in enumerate(state.messages):
        if m["id"] == edit_msg.id:
            updated = m.copy()
            updated["content"] = edit_msg.content
            updated["clock"] = state.vector_clock.copy()
            updated["timestamp"] = datetime.datetime.now().astimezone().isoformat()

            if not utils.quorum_write(updated):
                return {"error": "Quorum not achieved"}

            state.messages[i] = updated
            storage.append_to_wal(updated)

            return {"edited_at": config.NODE_ID, "message": updated}

    return {"error": "Message not found"}

# -------- REPLICATION --------

@app.post("/replicate")
def replicate(message: dict):
    state.vector_clock = storage.merge_vector_clocks(state.vector_clock, message.get("clock", {}))

    for i, m in enumerate(state.messages):
        if m["id"] == message["id"]:
            if storage.is_newer(message.get("clock", {}), m.get("clock", {}), message.get("timestamp", ""), m.get("timestamp", "")):
                state.messages[i] = message
                storage.append_to_wal(message)
            return {"status": "updated"}

    state.messages.append(message)
    storage.append_to_wal(message)

    return {"status": "added"}

# -------- READ --------

@app.get("/messages")
def get_messages():
    result = utils.quorum_read()

    if result is None:
        return {"error": "Quorum read failed"}

    sorted_msgs = sorted(result, key=lambda m: (sum(m.get("clock", {}).values()), m["id"]))

    return {"messages": sorted_msgs}