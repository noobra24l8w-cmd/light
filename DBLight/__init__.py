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
from .system import Light
from .MasterLight import MasterLight
from .flash import flash