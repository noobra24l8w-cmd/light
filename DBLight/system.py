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
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Set

CONTAINER_LOCATION = "./"
COMPRESSION_THRESHOLD = 1024 

class Light:
    __slots__ = (
        'containername', 'default_table', 'full_path', '_cache', 
        '_dirty_keys', '_lock', 'container', 'cursor'
    )

    _table_regex = re.compile(r"^[A-Za-z0-9_]+$")

    def __init__(self, containername: str = "main.db", table_name: str = "storage", path: str = CONTAINER_LOCATION):
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        
        self.containername = containername
        self.default_table = self._validate_table(table_name)
        self.full_path = str(path_obj / containername)
        
        self._lock = threading.RLock() 
        
        # Nested Cache: { "table_name": { "key": value } }
        self._cache: Dict[str, Dict[str, Any]] = {}
        # Nested Dirty Keys: { "table_name": { "key1", "key2" } }
        self._dirty_keys: Dict[str, Set[str]] = {}

        self.container = sqlite3.connect(self.full_path, check_same_thread=False)
        self.cursor = self.container.cursor()
        
        # Performance Tuning
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.cursor.execute("PRAGMA synchronous=NORMAL")
        self.cursor.execute("PRAGMA temp_store=MEMORY")
        
        self.create_table(self.default_table)

    def __enter__(self): return self
    def __exit__(self, *args): self.close()

    # ===============================
    # Helper & Safety
    # ===============================
    def _validate_table(self, name: Optional[str]) -> str:
        target = name or self.default_table
        if not self._table_regex.fullmatch(target):
            raise ValueError(f"Insecure table name: {target}")
        return target
    
    def _serialize(self, value: Any) -> Union[str, bytes]:
        # Ensure we are turning the wrapper/value into a JSON string first
        json_str = ujson.dumps(value) 
        
        # Now json_str is definitely a string, and len() will return an int
        if len(json_str) > COMPRESSION_THRESHOLD:
            return zlib.compress(json_str.encode("utf-8"))
        return json_str

    def _deserialize(self, value: Union[str, bytes]) -> Any:
        if isinstance(value, bytes):
            return ujson.loads(zlib.decompress(value).decode("utf-8"))
        return ujson.loads(value)

    def create_table(self, table_name: str):
        table = self._validate_table(table_name)
        with self._lock:
            self.cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table} (key TEXT PRIMARY KEY, value ANY)"
            )
            self.container.commit()
            if table not in self._cache:
                self._cache[table] = {}
                self._dirty_keys[table] = set()

    # ===============================
    # Core Operations
    # ===============================
    def get_value(self, key: str, table: str = None) -> Any:
        table = self._validate_table(table)
        with self._lock:
            # 1. Check specific table cache
            if table in self._cache and key in self._cache[table]:
                return self._cache[table][key]

            # 2. Check Database
            self.cursor.execute(f"SELECT value FROM {table} WHERE key = ?", (key,))
            row = self.cursor.fetchone()
            if row:
                val = self._deserialize(row[0])
                if table not in self._cache: self._cache[table] = {}
                self._cache[table][key] = val
                return val
        return None
    
    def set_value(self, key: str, value: Any, table: str = None):
        table = self._validate_table(table)
        with self._lock:
            if table not in self._cache:
                self._cache[table] = {}
                self._dirty_keys[table] = set()
            
            self._cache[table][key] = value
            self._dirty_keys[table].add(key)

    def set_multiple(self, mapping: Dict[str, Any], table: str = None):
        """
        Efficiently updates multiple keys in a specific table.
        Data stays in RAM until flush() is called.
        """
        table = self._validate_table(table)
        
        with self._lock:
            # 1. Ensure the table exists in our tracking systems
            if table not in self._cache:
                self._cache[table] = {}
                self._dirty_keys[table] = set()
            
            # 2. Update RAM cache and mark keys as dirty
            # We use .update() because it's implemented in C and very fast
            self._cache[table].update(mapping)
            self._dirty_keys[table].update(mapping.keys())

    def get_multiple(self, keys: List[str], table: str = None) -> Dict[str, Any]:
        table = self._validate_table(table)
        results = {}
        missing = []

        with self._lock:
            table_cache = self._cache.get(table, {})
            for k in keys:
                if k in table_cache: results[k] = table_cache[k]
                else: missing.append(k)

            if missing:
                for i in range(0, len(missing), 900): # Safe chunking for SQLite limits
                    chunk = missing[i:i+900]
                    placeholders = ",".join("?" for _ in chunk)
                    self.cursor.execute(
                        f"SELECT key, value FROM {table} WHERE key IN ({placeholders})", chunk
                    )
                    for r_key, r_val in self.cursor.fetchall():
                        decoded = self._deserialize(r_val)
                        results[r_key] = decoded
                        if table not in self._cache: self._cache[table] = {}
                        self._cache[table][r_key] = decoded
        return results

    def delete_multiple(self, keys: List[str], table: str = None):
        table = self._validate_table(table)
        with self._lock:
            for k in keys:
                if table in self._cache: self._cache[table].pop(k, None)
                if table in self._dirty_keys: self._dirty_keys[table].discard(k)
            
            for i in range(0, len(keys), 900):
                chunk = keys[i:i+900]
                placeholders = ",".join("?" for _ in chunk)
                self.cursor.execute(f"DELETE FROM {table} WHERE key IN ({placeholders})", chunk)
            self.container.commit()

    # ===============================
    # Extended Operations
    # ===============================
    def delete(self, key: str, table: str = None):
        """Removes a key from both memory and the database."""
        table = self._validate_table(table)
        with self._lock:
            # 1. Clear from Memory Cache
            if table in self._cache:
                self._cache[table].pop(key, None)
            
            # 2. Clear from Dirty Set (prevents accidental re-insertion on flush)
            if table in self._dirty_keys:
                self._dirty_keys[table].discard(key)

            # 3. Remove from Database
            self.cursor.execute(f"DELETE FROM {table} WHERE key = ?", (key,))
            self.container.commit()

    def check(self, key: str, table: str = None) -> bool:
        """Efficiently checks if a key exists without loading the full value."""
        table = self._validate_table(table)
        with self._lock:
            # 1. Check memory first (instant)
            if table in self._cache and key in self._cache[table]:
                return True

            # 2. Check Database (using EXISTS for maximum speed)
            self.cursor.execute(f"SELECT 1 FROM {table} WHERE key = ? LIMIT 1", (key,))
            return self.cursor.fetchone() is not None

    def list_keys(self, table: str = None) -> List[str]:
        """Returns all keys present in the specified table (Disk + Memory)."""
        table = self._validate_table(table)
        with self._lock:
            # 1. Fetch all keys currently on Disk
            self.cursor.execute(f"SELECT key FROM {table}")
            all_keys = {row[0] for row in self.cursor.fetchall()}

            # 2. Merge with keys currently in Memory (dirty cache)
            if table in self._cache:
                all_keys.update(self._cache[table].keys())

            return list(all_keys)

    # ===============================
    # Management
    
    def flush(self, table: str = None, clear_cache: bool = False):
        """Save dirty data to disk and optionally clear RAM cache."""
        with self._lock:
            tables_to_flush = [table] if table else list(self._dirty_keys.keys())
            
            for t_name in tables_to_flush:
                t_name = self._validate_table(t_name)
                dirty = self._dirty_keys.get(t_name)
                if not dirty: continue
                
                self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {t_name} (key TEXT PRIMARY KEY, value ANY)")
                
                batch = []
                for key in dirty:
                    if key in self._cache[t_name]:
                        batch.append((key, self._serialize(self._cache[t_name][key])))
                
                if batch:
                    self.cursor.executemany(
                        f"INSERT OR REPLACE INTO {t_name} (key, value) VALUES (?, ?)", batch
                    )
                self.container.commit()
                self._dirty_keys[t_name].clear()
                
                # IMPORTANT: This ensures the test can verify disk-writes
                if clear_cache:
                    self._cache[t_name].clear()
    
        return True
    
    def close(self):
        try:
            self.flush() # Flushes everything
        finally:
            with self._lock:
                self.container.close()

    def get_column(self, key: str, column: str, table: str = None) -> Any:
        """Fetches a specific field from a dictionary value."""
        try:
            data = self.get_value(key, table)
            if isinstance(data, dict):
                return data.get(column)
            return None
        except KeyError:
            return None

    def update_column(self, key: str, column: str, value: Any, table: str = None):
        """Updates a single field inside a dictionary value."""
        try:
            # 1. Get existing data
            data = self.get_value(key, table)
            if not isinstance(data, dict):
                data = {"value": data} # Wrap non-dicts to allow 'column' update
        except KeyError:
            # 2. If key doesn't exist, start new dict
            data = {}

        # 3. Update the specific field
        data[column] = value
        
        # 4. Save back to cache
        self.set_value(key, data, table)

    def list_tables(self) -> List[str]:
        """
        Queries the SQLite system schema to find all user-defined tables.
        Returns a list of table names.
        """
        with self._lock:
            # We query sqlite_master to find all 'table' types 
            # while excluding internal sqlite_ sequence/stat tables.
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            self.cursor.execute(query)
            
            # Fetchall returns a list of tuples like [('storage',), ('users',)]
            # We flatten it into a simple list of strings.
            tables = [row[0] for row in self.cursor.fetchall()]
            
            # Ensure the tables we found are also represented in our cache tracking
            for t in tables:
                if t not in self._cache:
                    self._cache[t] = {}
                    self._dirty_keys[t] = set()
                    
            return tables

    def clear_table(self, table: str):
        """Wipes all data from a table but keeps the table itself."""
        table = self._validate_table(table)
        with self._lock:
            # Clear Cache
            if table in self._cache: self._cache[table].clear()
            if table in self._dirty_keys: self._dirty_keys[table].clear()
            
            # Clear Disk
            self.cursor.execute(f"DELETE FROM {table}")
            self.container.commit()
            return True 

    def drop_table(self, table: str):
        """Completely deletes the table from the database file."""
        table = self._validate_table(table)
        with self._lock:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
            self.container.commit()
            
            # Remove from tracking
            self._cache.pop(table, None)
            self._dirty_keys.pop(table, None)
            return True

    def get_all(self, table: str = None) -> dict:
        """Returns the entire table as a dictionary."""
        table = self._validate_table(table)
        with self._lock:
            self.cursor.execute(f"SELECT key, value FROM {table}")
            rows = self.cursor.fetchall()
            # Deserialize each value while building the dict
            return {row[0]: self._deserialize(row[1]) for row in rows}

