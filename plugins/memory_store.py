from base import Plugin
from memcached_plugin import MemcachedPlugin

# we are going to do simple k/v storage in the memory pool
class MemoryMemcachedPlugin(MemcachedPlugin):
    memory_store = {}
    def __init__(self):
        super(MemoryMemcachedPlugin,self).__init__()

        # sum of value sizes
        self.mem_usage = 0.0

        # lookup of the len of the values in memory
        self.value_sizes = {}

    def _set_data(self, key, value):
        self.memory_store[key] = value
        self.value_sizes[key] = float(len(value))
        self.mem_usage += self.value_sizes[key]
        return True

    def _get_data(self, key):
        return self.memory_store.get(key)

    def _delete_data(self, key):
        self.mem_usage -= self.value_sizes[key]
        del self.value_sizes[key]
        del self.memory_store[key]
        return True
