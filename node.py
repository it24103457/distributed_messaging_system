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
    MAX_RETRIES = 3
    RETRY_DELAY = 2

    for attempt in range(MAX_RETRIES):
        if state.current_leader != config.SELF_URL:
            if state.current_leader is None:
                if attempt < MAX_RETRIES - 1:
                    import time
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    return {"error": "No leader elected yet. Automatic retry failed."}
            try:
                return requests.post(state.current_leader + "/send", json=msg.dict(), timeout=10).json()
            except Exception as e:
                print(f"[{config.NODE_ID}] Error forwarding to leader (attempt {attempt+1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    import time
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    return {"error": "Leader unavailable or election in progress. Automatic retry failed."}
        else:
            break

    with state.vector_clock_lock:
        state.vector_clock[config.SELF_URL] = state.vector_clock.get(config.SELF_URL, 0) + 1
        current_clock = state.vector_clock.copy()

    message = {
        "id": str(uuid.uuid4()),
        "sender": msg.sender,
        "receiver": msg.receiver,
        "content": msg.content,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "clock": current_clock
    }

    if not utils.quorum_write(message):
        return {"error": "Quorum not achieved"}

    storage.append_message(message)

    return {"stored_at": config.NODE_ID, "message": message}

# -------- EDIT --------

@app.put("/edit")
def edit(edit_msg: EditMessage):
    MAX_RETRIES = 3
    RETRY_DELAY = 2

    for attempt in range(MAX_RETRIES):
        if state.current_leader != config.SELF_URL:
            if state.current_leader is None:
                if attempt < MAX_RETRIES - 1:
                    import time
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    return {"error": "No leader elected yet. Automatic retry failed."}
            try:
                return requests.put(state.current_leader + "/edit", json=edit_msg.dict(), timeout=10).json()
            except Exception as e:
                print(f"[{config.NODE_ID}] Error forwarding edit to leader (attempt {attempt+1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    import time
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    return {"error": "Leader unavailable or election in progress. Automatic retry failed."}
        else:
            break

    with state.vector_clock_lock:
        state.vector_clock[config.SELF_URL] = state.vector_clock.get(config.SELF_URL, 0) + 1
        current_clock = state.vector_clock.copy()

    m = storage.get_message_by_id(edit_msg.id)
    if not m:
        return {"error": "Message not found"}

    m["content"] = edit_msg.content
    m["clock"] = current_clock
    m["timestamp"] = datetime.datetime.now().astimezone().isoformat()
    m["edited"] = True

    if not utils.quorum_write(m):
        return {"error": "Quorum not achieved"}

    storage.append_message(m)

    return {"edited_at": config.NODE_ID, "message": m}

# -------- REPLICATION --------

@app.post("/replicate")
def replicate(message: dict):
    with state.vector_clock_lock:
        state.vector_clock = storage.merge_vector_clocks(state.vector_clock, message.get("clock", {}))

    storage.append_message(message)

    return {"status": "processed"}

# -------- READ --------

@app.get("/messages")
def get_messages():
    result = utils.quorum_read()

    if result is None:
        return {"error": "Quorum read failed"}

    sorted_msgs = sorted(result, key=lambda m: (sum(m.get("clock", {}).values()), m["id"]))

    return {"messages": sorted_msgs}