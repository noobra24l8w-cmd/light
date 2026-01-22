"""
DBLight - High-Performance Sharded & Cached SQLite Engine
---------------------------------------------------------

Author: Light_dev
Gmail:  meofficial76@gmail.com
GitHub: https://github.com/noobra24l8w-cmd/light
Version: 0.1.0
License: Apache License 2.0 (see LICENSE file)

Description:
    A modular database stack featuring:
    - Flash: Ultra-fast RAM caching with TTL.
    - MasterLight: Horizontal scaling via SQLite sharding.
    - Light: Optimized single-file storage with Zlib compression.
    
    Perfect for low-resource environments (4GB RAM) and mobile systems.

Usage:
    from dblight import flash
    db = flash.flash(name="app_data", backend="master", path="./storage")
    db.set_value("user_1", {"data": "info"}, ttl=60)
    print(db.get_value("user_1"))
"""
import sqlite3
import ujson
import zlib
import threading
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from functools import lru_cache

# Configuration
MAX_SHARD_SIZE = 100 * 1024 * 1024 # 100MB for testing, adjust to 2GB for production
COMPRESSION_THRESHOLD = 1024

class RWLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._readers = 0
        self._writing = False

    def acquire_read(self):
        with self._lock:
            while self._writing: self._cond.wait()
            self._readers += 1

    def release_read(self):
        with self._lock:
            self._readers -= 1
            if self._readers == 0: self._cond.notify_all()

    def acquire_write(self):
        with self._lock:
            while self._writing or self._readers > 0: self._cond.wait()
            self._writing = True

    def release_write(self):
        with self._lock:
            self._writing = False
            self._cond.notify_all()

class Shard:
    def __init__(self, db_path: Path):
        self.path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous = OFF")
        self._cache = {} # {table: {key: value}}
        self._dirty = {} # {table: set(keys)}
        self._lock = threading.RLock()
        self._last_size = os.path.getsize(self.path) if self.path.exists() else 0

    def estimate_size(self): 
        return self._last_size

    def _serialize(self, v):
        j = ujson.dumps(v)
        return zlib.compress(j.encode()) if len(j) > COMPRESSION_THRESHOLD else j

    def _deserialize(self, b):
        if isinstance(b, bytes): return ujson.loads(zlib.decompress(b).decode())
        return ujson.loads(b)

class MasterLight:
    def __init__(self, base_path="./data_storage", default_table="storage"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.default_table = default_table
        
        # Core Locking and API attributes
        self._rwlock = RWLock()
        self._lock = threading.RLock() # Compatibility for Flash/Direct calls
        self._cache = {} # Compatibility attribute to prevent AttributeErrors
        
        # Master Index Connection
        self.index_conn = sqlite3.connect(self.base_path / "master_index.db", check_same_thread=False)
        self.index_conn.execute("PRAGMA journal_mode=WAL")
        self.index_conn.execute("PRAGMA synchronous = OFF") 
        self.index_conn.execute("CREATE TABLE IF NOT EXISTS idx (table_name TEXT, key TEXT, shard_id INTEGER, PRIMARY KEY(table_name, key))")
        
        self.shards: Dict[int, 'Shard'] = {}
        self.active_shard_id = self._get_last_shard_id()

    def _get_last_shard_id(self) -> int:
        cursor = self.index_conn.execute("SELECT MAX(shard_id) FROM idx")
        res = cursor.fetchone()
        return res[0] if res and res[0] is not None else 0

    def _get_shard(self, shard_id: int) -> 'Shard':
        if shard_id not in self.shards:
            self.shards[shard_id] = Shard(self.base_path / f"shard_{shard_id}.db")
        return self.shards[shard_id]

    @lru_cache(maxsize=10000)
    def _lookup_shard_id(self, table: str, key: str) -> Optional[int]:
        cursor = self.index_conn.execute("SELECT shard_id FROM idx WHERE table_name = ? AND key = ?", (table, key))
        row = cursor.fetchone()
        return row[0] if row else None

    # --- THE 15 CORE METHODS ---

    # 1. set_value
    def set_value(self, key: str, value: Any, table: str = None):
        self.set_multiple({key: value}, table)

    # 2. get_value
    def get_value(self, key: str, table: str = None, default=None):
        t = table or self.default_table
        self._rwlock.acquire_read()
        try:
            sid = self._lookup_shard_id(t, key)
            if sid is None: return default
            
            shard = self._get_shard(sid)
            with shard._lock:
                if t in shard._cache and key in shard._cache[t]:
                    return shard._cache[t][key]
                
                # Disk fetch
                shard.conn.execute(f"CREATE TABLE IF NOT EXISTS {t} (key TEXT PRIMARY KEY, value ANY)")
                row = shard.conn.execute(f"SELECT value FROM {t} WHERE key=?", (key,)).fetchone()
                if row:
                    val = shard._deserialize(row[0])
                    if t not in shard._cache: shard._cache[t] = {}
                    shard._cache[t][key] = val
                    return val
            return default
        finally: self._rwlock.release_read()

    # 3. set_multiple
    def set_multiple(self, mapping: Dict[str, Any], table: str = None):
        t = table or self.default_table
        self._rwlock.acquire_write()
        try:
            self.index_conn.execute("BEGIN")
            groups = {}
            new_idx = []
            for k, v in mapping.items():
                sid = self._lookup_shard_id(t, k)
                if sid is None:
                    # Shard rotation logic
                    if self._get_shard(self.active_shard_id).estimate_size() > MAX_SHARD_SIZE:
                        self.active_shard_id += 1
                    sid = self.active_shard_id
                    new_idx.append((t, k, sid))
                    self._lookup_shard_id.cache_clear()
                
                if sid not in groups: groups[sid] = {}
                groups[sid][k] = v
            
            if new_idx:
                self.index_conn.executemany("INSERT OR REPLACE INTO idx VALUES (?,?,?)", new_idx)
            self.index_conn.commit()

            # Pass to shards
            for sid, dmap in groups.items():
                shard = self._get_shard(sid)
                with shard._lock:
                    if t not in shard._cache: shard._cache[t], shard._dirty[t] = {}, set()
                    shard._cache[t].update(dmap)
                    shard._dirty[t].update(dmap.keys())
        finally: self._rwlock.release_write()

    # 4. get_multiple
    def get_multiple(self, keys: List[str], table: str = None) -> Dict[str, Any]:
        t = table or self.default_table
        results = {}
        shard_groups = {}
        
        self._rwlock.acquire_read()
        try:
            for k in keys:
                sid = self._lookup_shard_id(t, k)
                if sid is not None:
                    if sid not in shard_groups: shard_groups[sid] = []
                    shard_groups[sid].append(k)
            
            for sid, skeys in shard_groups.items():
                shard = self._get_shard(sid)
                with shard._lock:
                    tc = shard._cache.get(t, {})
                    miss = []
                    for k in skeys:
                        if k in tc: results[k] = tc[k]
                        else: miss.append(k)
                    
                    if miss:
                        shard.conn.execute(f"CREATE TABLE IF NOT EXISTS {t} (key TEXT PRIMARY KEY, value ANY)")
                        for i in range(0, len(miss), 999):
                            chunk = miss[i:i+999]
                            qs = ",".join("?"*len(chunk))
                            rows = shard.conn.execute(f"SELECT key, value FROM {t} WHERE key IN ({qs})", chunk).fetchall()
                            for rk, rv in rows:
                                val = shard._deserialize(rv)
                                results[rk] = val
                                if t not in shard._cache: shard._cache[t] = {}
                                shard._cache[t][rk] = val
            return results
        finally: self._rwlock.release_read()

    # 5. delete
    def delete(self, key: str, table: str = None):
        self.delete_multiple([key], table)

    # 6. delete_multiple
    def delete_multiple(self, keys: List[str], table: str = None):
        t = table or self.default_table
        self._rwlock.acquire_write()
        try:
            groups = {}
            for k in keys:
                sid = self._lookup_shard_id(t, k)
                if sid is not None:
                    if sid not in groups: groups[sid] = []
                    groups[sid].append(k)
            
            for sid, skeys in groups.items():
                shard = self._get_shard(sid)
                with shard._lock:
                    if t in shard._cache:
                        for k in skeys: shard._cache[t].pop(k, None)
                    if t in shard._dirty:
                        for k in skeys: shard._dirty[t].discard(k)
                    
                    for i in range(0, len(skeys), 999):
                        chunk = skeys[i:i+999]
                        qs = ",".join("?"*len(chunk))
                        shard.conn.execute(f"DELETE FROM {t} WHERE key IN ({qs})", chunk)
                    shard.conn.commit()
            
            if keys:
                self.index_conn.executemany("DELETE FROM idx WHERE table_name=? AND key=?", [(t, k) for k in keys])
                self.index_conn.commit()
                self._lookup_shard_id.cache_clear()
        finally: self._rwlock.release_write()

    # 7. check
    def check(self, key: str, table: str = None) -> bool:
        t = table or self.default_table
        self._rwlock.acquire_read()
        try:
            return self._lookup_shard_id(t, key) is not None
        finally: self._rwlock.release_read()

    # 8. list_keys
    def list_keys(self, table: str = None) -> List[str]:
        t = table or self.default_table
        self._rwlock.acquire_read()
        try:
            cursor = self.index_conn.execute("SELECT key FROM idx WHERE table_name = ?", (t,))
            return [row[0] for row in cursor.fetchall()]
        finally: self._rwlock.release_read()

    # 9. list_tables
    def list_tables(self) -> List[str]:
        self._rwlock.acquire_read()
        try:
            cursor = self.index_conn.execute("SELECT DISTINCT table_name FROM idx")
            return [row[0] for row in cursor.fetchall()]
        finally: self._rwlock.release_read()

    # 10. create_table
    def create_table(self, table_name: str):
        self._rwlock.acquire_write()
        try:
            shard = self._get_shard(self.active_shard_id)
            shard.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (key TEXT PRIMARY KEY, value ANY)")
            return True
        finally: self._rwlock.release_write()

    # 11. clear_table
    def clear_table(self, table: str):
        self._rwlock.acquire_write()
        try:
            self.index_conn.execute("DELETE FROM idx WHERE table_name = ?", (table,))
            self.index_conn.commit()
            for shard in self.shards.values():
                with shard._lock:
                    shard.conn.execute(f"DROP TABLE IF EXISTS {table}")
                    shard.conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (key TEXT PRIMARY KEY, value ANY)")
                    if table in shard._cache: shard._cache[table].clear()
                    if table in shard._dirty: shard._dirty[table].clear()
            self._lookup_shard_id.cache_clear()
            return True
        finally: self._rwlock.release_write()

    # 12. drop_table
    def drop_table(self, table: str):
        self._rwlock.acquire_write()
        try:
            self.index_conn.execute("DELETE FROM idx WHERE table_name = ?", (table,))
            self.index_conn.commit()
            for shard in self.shards.values():
                with shard._lock:
                    shard.conn.execute(f"DROP TABLE IF EXISTS {table}")
                    shard._cache.pop(table, None)
                    shard._dirty.pop(table, None)
            self._lookup_shard_id.cache_clear()
            return True
        finally: self._rwlock.release_write()

    # 13. flush
    def flush(self, clear_cache=False):
        self._rwlock.acquire_write()
        try:
            for shard in self.shards.values():
                with shard._lock:
                    for t, ks in shard._dirty.items():
                        if not ks: continue
                        shard.conn.execute("BEGIN")
                        shard.conn.execute(f"CREATE TABLE IF NOT EXISTS {t} (key TEXT PRIMARY KEY, value ANY)")
                        batch = [(k, shard._serialize(shard._cache[t][k])) for k in ks]
                        shard.conn.executemany(f"INSERT OR REPLACE INTO {t} VALUES (?,?)", batch)
                        shard.conn.commit()
                        ks.clear()
                    if clear_cache: shard._cache.clear()
                    shard._last_size = os.path.getsize(shard.path)
            return True
        finally: self._rwlock.release_write()

    # 14. close
    def close(self):
        self.flush()
        self.index_conn.close()
        for s in self.shards.values(): s.conn.close()

    # 15. get_all
    def get_all(self, table: str = None) -> dict:
        t = table or self.default_table
        combined_data = {}
        self._rwlock.acquire_read()
        try:
            # Iterate all physical shard files to ensure no data is missed
            for f in self.base_path.glob("shard_*.db"):
                try:
                    sid = int(f.stem.split('_')[1])
                    shard = self._get_shard(sid)
                    rows = shard.conn.execute(f"SELECT key, value FROM {t}").fetchall()
                    for rk, rv in rows:
                        combined_data[rk] = shard._deserialize(rv)
                except (sqlite3.OperationalError, ValueError):
                    continue
            return combined_data
        finally: self._rwlock.release_read()

