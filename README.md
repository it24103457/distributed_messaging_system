<div align="center">
  <h1>⚡ Distributed Fault-Tolerant Messaging System</h1>
  <p><i>A highly scalable, eventually consistent distributed messaging system built in Python using FastAPI.</i></p>

  [![Python](https://img.shields.io/badge/Python-3.8+-blue.svg?logo=python&logoColor=white)](#)
  [![FastAPI](https://img.shields.io/badge/FastAPI-100%25-009688.svg?logo=fastapi&logoColor=white)](#)
  [![Uvicorn](https://img.shields.io/badge/Uvicorn-0.23.2-purple.svg?logo=uvicorn&logoColor=white)](#)
  [![Architecture](https://img.shields.io/badge/Architecture-Distributed-ff69b4.svg)](#)
</div>

---

## 👥 Team Members

| Name                   | Registration Number  |         Email            |
|------------------------|----------------------|--------------------------|
| J.M.N.V.B. Karunaratne |     [IT24103457]     | [IT24103457@my.sliit.lk] |
| R.M.K.M. Chathuranga   |     [IT24103441]     | [IT24103441@my.sliit.lk] |
| D.M.N. Pesanjith       |     [IT24101505]     | [IT24101505@my.sliit.lk] |
| S.T. Senadheera        |     [IT24103442]     | [IT24103442@my.sliit.lk] |

> This project was developed collaboratively as part of the Distributed Systems group assignment.


## 🚀 Core Features

* 👑 **Raft Consensus Leader Election**: Automatic background failover guarantees that if the primary node crashes, a new leader is seamlessly elected in ~1-2 seconds with zero data loss.

* 💾 **Write-Ahead Log (WAL) & Snapshotting**: Every transaction is securely logged to disk (`wal_XXXX.jsonl`). To prevent unbounded log growth, the system natively implements automated log compaction that serializes memory into `snapshot_XXXX.json` every 50 operations.

* ⏱️ **Vector Clocks (Logical Time)**: Eliminates dependence on physical system time (which is prone to clock drift) by tracking causality and message sequences using reliable vector clocks.

* ⚡ **Concurrent Quorum Replication**: A custom `ThreadPoolExecutor` layer handles node broadcasting to replicate data concurrently ($O(1)$) rather than sequentially ($O(N)$), massively reducing network latency.

* 🛡️ **Application-Level Failover**: If a client pings a crashed node mid-election, the system actively queues and retries forwarding the message with an integrated 3-attempt retry loop to avoid 500-level fatal errors.

---


## 💻 Getting Started

### 1. Boot up the Cluster
Open **PowerShell** in the root directory and run the booting script:
```powershell
./run_nodes.ps1
```
> **Note:** This script verifies your dependencies and launches 5 independent background terminal systems connected on ports `5001` through `5005` to simulate a true distributed architecture!

### 2. Run the Stress Test
To easily verify cluster limits, parallelism, and WAL Compaction features, run the custom load testing tool inside a standard terminal window:
```powershell
python test_load.py
```
> *This utility will fire 500 deep-transaction bulk messages at your system dynamically across 100 asynchronous threads.*

---

## 📡 API Documentation

### `POST /send`
Broadcast a message to the cluster and achieve a replicated quorum save state.
```json
{
  "sender": "User 1",
  "receiver": "User 2",
  "content": "Hello, Distributed World!"
}
```

### `PUT /edit`
Safely updates the target message and pushes new logical vector timestamps down to all replicas.
```json
{
  "id": "message-uuid-here",
  "content": "Updated content!"
}
```

### `GET /messages`
Initiates a cluster-wide Quorum Read and merges all inputs causally to return the exact final state of the network.
