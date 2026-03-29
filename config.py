import os

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
