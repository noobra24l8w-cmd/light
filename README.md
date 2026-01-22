Markdown
# DBLight Engine 🚀

DBLight Engine is a lightweight, high-performance database system built on top of SQLite.  
It provides a **unified API** for local storage, **multi-file sharding**, and **ultra-fast in-memory (RAM) caching**, designed to work efficiently even on low-resource devices.

---

## ✨ Features

- ⚡ **RAM Flash Layer** for instant reads  
- 🧩 **Sharded SQLite Storage** for scalable disk persistence  
- 🔒 **Thread-Safe Concurrency** using a custom RWLock  
- ⏱ **TTL (Time-To-Live)** support for cached keys  
- 📱 Optimized for **mobile & low-memory systems**  
- 🔁 Unified API for cache + disk operations  

---

## 📦 Installation

```bash
pip install light-db-engine
```
Requires Python 3.8+

---

## 📊 Performance & Scaling

DBLight is optimized for high-efficiency usage without exhausting system memory.

Benchmark (example):

Speed: ~8,800 keys/second

Records: 1,000,000

Environment: Android device (4GB RAM), SQLite WAL enabled

Workload: Single-threaded write test


Design Highlights

Sharded disk writes reduce SQLite file locking

Hot data is served directly from RAM

Disk I/O is batched for efficiency



---

## 🧠 Architecture: The Three Engines

| Engine      | Name           | Best For                                           |
| :---------- | :------------- | :------------------------------------------------- |
| Flash       | Hybrid Layer   | Ultra-fast RAM access with TTL; sits on top.       |
| MasterLight | Sharded Disk   | Large datasets (1M+ keys) split across files.      |
| Light       | Standard Disk  | Simple, single-file persistent SQLite storage.     |
---

## 🚀 Quick Start (Hybrid Mode)

```Python
from DBLight.flash import flash

db = flash(
    name="app_data",
    backend="master",
    path="./storage"
)

db.set_value(
    "session_001",
    {"user": "admin", "roles": ["editor"]},
    ttl=60
)

data = db.get_value("session_001")
print("Retrieved:", data)

db.flush()
```

---

## 🔧 Backends

Backend	Description

master	Sharded SQLite persistent backend
flash	In-memory cache layer


> Additional backends may be added in future versions.




---

⚖️ License

This project is licensed under the Apache License 2.0.

You are free to use, modify, and distribute this software for any purpose, including commercial use.

Original authorship and copyright remain with the author.

See the LICENSE file for full terms.


---

👤 Author

Original Author: meofficial76@gmail.com


---

🛠 Roadmap

Async I/O support

Multi-process safe locking

Optional compression

Metrics & monitoring hooks



---

⭐ Contributing

Contributions, bug reports, and improvements are welcome.


---

📫 Support

For issues or feature requests, open an issue in the repository.


---

DBLight Engine — Fast. Simple. Reliable.

---
