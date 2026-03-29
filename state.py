import time
import config

messages = []
vector_clock = {peer: 0 for peer in config.PEERS}
WAL_FILE = f"wal_{config.PORT}.jsonl"

active_peers = set()
current_leader = None

node_state = "FOLLOWER"
current_term = 0
voted_for = None
last_heartbeat = time.time()
