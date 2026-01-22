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
import threading
import time
import signal
import sys
from collections import OrderedDict

class flash:
    __slots__ = (
        "memory", "_cache", "_deleted", "db", "_lock", 
        "max_keys", "default_table", "_watchers", 
        "_middleware", "_ttl_thread_stop"
    )

    def __init__(self, name="data", backend="light", path="./", 
                 default_table="storage", max_keys=100000, preload=False):
        """
        max_keys: Maximum keys per table to keep in RAM.
        preload: If True, loads data from disk on startup (up to max_keys).
        """
        self._lock = threading.RLock()
        self.max_keys = max_keys
        self.default_table = default_table
        
        # Structure: { "table": OrderedDict({ "key": wrapper }) }
        self.memory = {} 
        self._cache = {}   
        self._deleted = {} 
        
        self._watchers = {}   # { key: [callbacks] }
        self._middleware = [] # [functions]
        self._ttl_thread_stop = threading.Event()

        # 1. Initialize Backend
        if backend == "master":
            from .MasterLight import MasterLight
            self.db = MasterLight(base_path=path, default_table=default_table)
        else:
            from .system import Light
            self.db = Light(containername=f"{name}.db", path=path, table_name=default_table)

        # 2. Feature: Preload Option
        if preload:
            self._preload_data()

        # 3. Start Background TTL Monitor
        self._start_ttl_monitor()

    # ==========================
    # FEATURE: Middleware & Watchers
    # ==========================
    def use(self, func):
        """Add middleware: func(key, value) -> processed_value"""
        self._middleware.append(func)

    def watch(self, key, callback):
        """Trigger callback(value) when key changes."""
        with self._lock:
            if key not in self._watchers: self._watchers[key] = []
            self._watchers[key].append(callback)

    # ==========================
    # FEATURE: RAM Limiting (LRU)
    # ==========================
    def _manage_memory(self, table):
        """Evicts the least recently used item if RAM is full."""
        if len(self.memory[table]) > self.max_keys:
            # Pop the oldest item (Least Recently Used)
            # We try to find one that isn't 'dirty' (in _cache) if possible, 
            # but for speed, we pop the oldest.
            self.memory[table].popitem(last=False)

    # ==========================
    # CORE API
    # ==========================
    def set_value(self, key, value, table=None, ttl=None):
        t = table or self.default_table
        
        # 1. Run Middleware
        for mw in self._middleware:
            value = mw(key, value)

        # 2. Create Wrapper (TTL)
        expiry = time.time() + ttl if ttl else None
        wrapper = {"val": value, "ttl": expiry}

        with self._lock:
            if t not in self.memory: self.memory[t] = OrderedDict()
            if t not in self._cache: self._cache[t] = {}
            if t not in self._deleted: self._deleted[t] = set()

            # 3. Store in RAM + Mark Dirty
            self.memory[t][key] = wrapper
            self._cache[t][key] = wrapper
            self._deleted[t].discard(key)
            
            # 4. Handle LRU & Watchers
            self.memory[t].move_to_end(key)
            self._manage_memory(t)
            
            if key in self._watchers:
                for cb in self._watchers[key]: cb(value)

    def get_value(self, key, table=None, default=None):
        t = table or self.default_table
        if t not in self.memory: self.memory[t] = OrderedDict()

        # 1. Check RAM
        item = self.memory[t].get(key)

        # 2. If MISS -> Check Disk (Lazy Load)
        if item is None:
            try:
                raw = self.db.get_value(key, table=t)
                # Convert old data to wrapper format
                item = raw if (isinstance(raw, dict) and "val" in raw) else {"val": raw, "ttl": None}
                
                with self._lock:
                    self.memory[t][key] = item
                    self._manage_memory(t)
            except (KeyError, Exception):
                return default

        # 3. Check TTL
        if isinstance(item, dict) and item.get("ttl") is not None:
            if time.time() > item["ttl"]:
                self.delete(key, table=t)
                return default
        
        # Update LRU Position
        self.memory[t].move_to_end(key)
        return item["val"] if isinstance(item, dict) else item

    # ==========================
    # PRELOAD & MAINTENANCE
    # ==========================
    def _preload_data(self):
  
        for t in self.db.list_tables():
            keys = self.db.list_keys(table=t)[:self.max_keys]
            if keys:
                data = self.db.get_multiple(keys, table=t)
                self.memory[t] = OrderedDict(data)
        return True

    def flush(self):
        """Saves all dirty memory to the backend disk."""
        with self._lock:
            for t_name, keys_to_del in self._deleted.items():
                if keys_to_del:
                    self.db.delete_multiple(list(keys_to_del), table=t_name)
                    keys_to_del.clear()

            for t_name, dirty_data in self._cache.items():
                if dirty_data:
                    self.db.set_multiple(dirty_data, table=t_name)
                    dirty_data.clear()
            self.db.flush()
        return True

    def _start_ttl_monitor(self):
        def run():
            while not self._ttl_thread_stop.is_set():
                time.sleep(30)
                now = time.time()
                with self._lock:
                    for t_name, table_dict in self.memory.items():
                        expired = [k for k, v in table_dict.items() 
                                   if isinstance(v, dict) and v.get("ttl") is not None and now > v["ttl"]]
                        for k in expired: self.delete(k, table=t_name)
        threading.Thread(target=run, daemon=True).start()

    def close(self):
        self._ttl_thread_stop.set()
        self.flush()
        self.db.close()

    def __enter__(self): return self
    def __exit__(self, *args): self.close()

    def delete(self, key, table=None):
        t = table or self.default_table
        with self._lock:
            if t in self.memory: self.memory[t].pop(key, None)
            if t in self._cache: self._cache[t].pop(key, None)
            if t not in self._deleted: self._deleted[t] = set()
            self._deleted[t].add(key)

    # ===============================
    # Bulk Operations
    # ===============================
    def set_multiple(self, mapping: dict, table: str = None, ttl: None = None):
        """Updates many keys at once. Respects RAM limits and Watchers."""
        t = table or self.default_table
        expiry = time.time() + ttl if ttl else None
        
        with self._lock:
            if t not in self.memory: self.memory[t] = OrderedDict()
            if t not in self._cache: self._cache[t] = {}
            if t not in self._deleted: self._deleted[t] = set()

            for key, value in mapping.items():
                # 1. Run Middleware
                for mw in self._middleware:
                    value = mw(key, value)
                
                wrapper = {"val": value, "ttl": expiry}
                
                # 2. Update RAM and Cache
                self.memory[t][key] = wrapper
                self._cache[t][key] = wrapper
                self._deleted[t].discard(key)
                self.memory[t].move_to_end(key)

                # 3. Trigger Watchers
                if key in self._watchers:
                    for cb in self._watchers[key]: cb(value)

            # 4. Final Memory Check
            while len(self.memory[t]) > self.max_keys:
                self.memory[t].popitem(last=False)

    def get_multiple(self, keys: list, table: str = None) -> dict:
        """Fetches multiple keys. Tries RAM first, then Disk."""
        results = {}
        missing = []
        t = table or self.default_table

        with self._lock:
            if t not in self.memory: self.memory[t] = OrderedDict()
            
            for k in keys:
                val = self.get_value(k, table=t) # Logic inside get_value handles Disk/TTL
                if val is not None:
                    results[k] = val
        return results

    def delete_multiple(self, keys: list, table: str = None):
        """Removes multiple keys from RAM and queues for Disk deletion."""
        t = table or self.default_table
        with self._lock:
            if t not in self._deleted: self._deleted[t] = set()
            for k in keys:
                if t in self.memory: self.memory[t].pop(k, None)
                if t in self._cache: self._cache[t].pop(k, None)
                self._deleted[t].add(k)

    # ===============================
    # Discovery & Maintenance
    # ===============================
    def list_keys(self, table: str = None) -> list:
        """Returns all keys from Disk + current Memory."""
        t = table or self.default_table
        # 1. Get disk keys
        disk_keys = set(self.db.list_keys(table=t))
        # 2. Merge with RAM keys
        with self._lock:
            ram_keys = set(self.memory.get(t, {}).keys())
            deleted_keys = self._deleted.get(t, set())
            
        return list((disk_keys | ram_keys) - deleted_keys)

    def list_tables(self) -> list:
        """Returns list of all tables in the database."""
        return self.db.list_tables()

    def check(self, key: str, table: str = None) -> bool:
        """Checks if a key exists without loading full value into RAM."""
        t = table or self.default_table
        if t in self.memory and key in self.memory[t]:
            return True
        return self.db.check(key, table=t)

    def create_table(self, name: str):
        """Creates a table in the backend and prepares RAM cache."""
        with self._lock:
            # Prepare RAM
            if name not in self.memory:
                self.memory[name] = OrderedDict()
                self._cache[name] = {}
                self._deleted[name] = set()
            
            # Prepare Disk
            self.db.create_table(name)
            return True

    def clear_table(self, table: str):
        """Wipes a table in RAM and Disk."""
        t = table
        with self._lock:
            if t in self.memory: self.memory[t].clear()
            if t in self._cache: self._cache[t].clear()
            self.db.clear_table(t)

    def drop_table(self, table: str):
        """Deletes a table completely."""
        t = table
        with self._lock:
            self.memory.pop(t, None)
            self._cache.pop(t, None)
            self._deleted.pop(t, None)
            self.db.drop_table(t)

    def set_multiple(self, mapping: dict, table: str = None, ttl: int = None):
        """Updates many keys at once. Triggers middleware, watchers, and LRU."""
        t = table or self.default_table
        expiry = time.time() + ttl if ttl else None
        
        with self._lock:
            # Ensure table exists in RAM
            if t not in self.memory: self.memory[t] = OrderedDict()
            if t not in self._cache: self._cache[t] = {}
            if t not in self._deleted: self._deleted[t] = set()

            for key, value in mapping.items():
                # 1. Run Middleware
                for mw in self._middleware:
                    value = mw(key, value)
                
                # 2. Create Wrapper
                wrapper = {"val": value, "ttl": expiry}
                
                # 3. Update RAM and Change-Tracking
                self.memory[t][key] = wrapper
                self._cache[t][key] = wrapper
                self._deleted[t].discard(key)
                self.memory[t].move_to_end(key) # Mark as recently used

                # 4. Trigger Watchers
                if key in self._watchers:
                    for cb in self._watchers[key]:
                        try: cb(value)
                        except: pass

            # 5. Final Memory Check (Pop oldest if over limit)
            while len(self.memory[t]) > self.max_keys:
                self.memory[t].popitem(last=False)

    def get_multiple(self, keys: list, table: str = None) -> dict:
        """Fetches many keys. Uses RAM hits for speed and Disk for misses."""
        results = {}
        t = table or self.default_table
        
        # We reuse the logic in get_value to handle TTL and Disk-loading automatically
        for k in keys:
            val = self.get_value(k, table=t)
            if val is not None:
                results[k] = val
        return results

    def delete_multiple(self, keys: list, table: str = None):
        """Removes many keys and marks them for Disk deletion on next flush."""
        t = table or self.default_table
        with self._lock:
            if t not in self.memory: self.memory[t] = OrderedDict()
            if t not in self._cache: self._cache[t] = {}
            if t not in self._deleted: self._deleted[t] = set()

            for k in keys:
                self.memory[t].pop(k, None)
                self._cache[t].pop(k, None)
                self._deleted[t].add(k)

    def get_all(self, table: str = None) -> dict:
        """Returns every key-value pair from RAM and Disk (respecting TTL)."""
        t = table or self.default_table
        
        # 1. Get everything from the underlying Disk (Light or MasterLight)
        all_data = self.db.get_all(table=t)
        
        # 2. Update/Overwrite with what is currently in RAM
        with self._lock:
            if t in self.memory:
                for k, wrapper in self.memory[t].items():
                    # Only include if not expired
                    if isinstance(wrapper, dict) and wrapper.get("ttl"):
                        if time.time() > wrapper["ttl"]:
                            continue
                    
                    # Unwrap and add to the final result
                    all_data[k] = wrapper["val"] if isinstance(wrapper, dict) and "val" in wrapper else wrapper
                    
            # 3. Remove keys marked for deletion in Flash but not yet flushed
            if t in self._deleted:
                for k in self._deleted[t]:
                    all_data.pop(k, None)
                    
        return all_data

