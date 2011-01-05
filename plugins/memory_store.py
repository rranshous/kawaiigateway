from base import Plugin

####################################
# ty to github.com/superisaac/redqueue
####################################

from memcached_plugin import MemcachedPlugin

# we are going to do simple k/v storage in the memory pool
class MemoryMemcachedPlugin(MemcachedPlugin):
    memory_store = {}

    def _set_data(self, key, value):
        self.memory_store[key] = value
        return True

    def _get_data(self, key):
        return self.memory_store.get(key)

    def _delete_data(self, key):
        del self.memory_store[key]
        return True
