import time
import random
import requests
import config
import state

def reset_election_timeout():
    state.last_heartbeat = time.time()

def get_election_timeout():
    return random.uniform(1.0, 2.0)

def send_heartbeats():
    alive = []
    
    for peer in config.PEERS:
        if peer == config.SELF_URL:
            continue
        try:
            res = requests.post(peer + "/append_entries", json={
                "term": state.current_term,
                "leader_id": config.SELF_URL
            }, timeout=0.2)
            if res.status_code == 200:
                data = res.json()
                if data.get("term", 0) > state.current_term:
                    state.current_term = data.get("term")
                    state.node_state = "FOLLOWER"
                    state.voted_for = None
                    return alive
                elif data.get("success"):
                    alive.append(peer)
        except:
            pass
            
    return alive

def election_timer_task():
    timeout = get_election_timeout()
    
    while True:
        time.sleep(0.1)
        
        if state.node_state == "LEADER":
            continue
            
        if time.time() - state.last_heartbeat > timeout:
            print(f"[{config.NODE_ID}] Election timeout! Starting election for term {state.current_term + 1}", flush=True)
            state.node_state = "CANDIDATE"
            state.current_term += 1
            state.voted_for = config.SELF_URL
            votes_received = 1
            reset_election_timeout()
            timeout = get_election_timeout()
            
            for peer in config.PEERS:
                if peer == config.SELF_URL:
                    continue
                try:
                    res = requests.post(peer + "/request_vote", json={
                        "term": state.current_term,
                        "candidate_id": config.SELF_URL
                    }, timeout=0.2)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("term", 0) > state.current_term:
                            print(f"[{config.NODE_ID}] Stepping down. {peer} has term {data.get('term')} > my {state.current_term}", flush=True)
                            state.current_term = data.get("term")
                            state.node_state = "FOLLOWER"
                            state.voted_for = None
                            reset_election_timeout()
                            break
                        if data.get("vote_granted"):
                            print(f"[{config.NODE_ID}] Got vote from {peer}", flush=True)
                            votes_received += 1
                        else:
                            print(f"[{config.NODE_ID}] Vote rejected by {peer}. Data: {data}", flush=True)
                except Exception as e:
                    print(f"[{config.NODE_ID}] Error requesting vote from {peer}: {type(e).__name__}", flush=True)
            
            print(f"[{config.NODE_ID}] Election for term {state.current_term} ended. Votes: {votes_received}/{config.QUORUM}, state={state.node_state}", flush=True)
            if state.node_state == "CANDIDATE" and votes_received >= config.QUORUM:
                print(f"[{config.NODE_ID}] Won election! I am the new leader for term {state.current_term}", flush=True)
                state.node_state = "LEADER"
                state.current_leader = config.SELF_URL
                
                # Immediately send heartbeats
                send_heartbeats()

def heartbeat_task():
    while True:
        if state.node_state == "LEADER":
            alive = send_heartbeats()
            state.active_peers.clear()
            state.active_peers.update(alive)
            
        time.sleep(0.3)
