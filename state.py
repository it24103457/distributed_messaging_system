import time
import config
import threading

messages = []
vector_clock = {peer: 0 for peer in config.PEERS}
vector_clock_lock = threading.Lock()
WAL_FILE = f"wal_{config.PORT}.jsonl"
SNAPSHOT_FILE = f"snapshot_{config.PORT}.json"
COMPACTION_THRESHOLD = 50
wal_count = 0

active_peers = set()
current_leader = None

node_state = "FOLLOWER"
current_term = 0
voted_for = None
last_heartbeat = time.time()
