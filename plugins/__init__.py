from memory_store import MemoryMemcachedPlugin
from disk_store import DiskMemcachedPlugin
from queue import QueuePlugin

# there is signifigance to the order
plugins = [
    QueuePlugin,
    MemoryMemcachedPlugin
 #   DiskMemcachedPlugin
]
