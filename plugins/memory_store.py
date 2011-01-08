from base import Plugin
from memcached_plugin import MemcachedPlugin
import logging

class LoggingDict(dict):
    """ We log the order theys were added """
    def __init__(self,*args,**kwargs):
        super(LoggingDict,self).__init__(*args,**kwargs)
        self.key_list = []

    def __setitem__(self,k,*args,**kwargs):
        self.key_list.append(k)
        return super(LoggingDict,self).__setitem__(k,*args,**kwargs)

    def __delitem__(self,k,*args,**kwargs):
        self.key_list.remove(k)
        return super(LoggingDict,self).__delitem__(k,*args,**kwargs)

class MemoryStore(object):
    memory_store = {}
    cmdline_options = [
        (('memory_limit',),{'help':'MB caching max, -1 = no limit',
                            'default':100,'type':int})
    ]
    def __init__(self,memory_limit=None):

        # sum of value sizes
        self.memory_usage = 0.0

        # lookup of the len of the values in memory
        self.value_sizes = LoggingDict()

        # limit of memory pool values in MB
        self.memory_limit = memory_limit or -1

    def cull_pool(self):
        """ if we are over the memory limit remove
            the oldest k/v pairs until we are back under """

        # if the limit is -1 than there is no limit !
        if self.memory_limit is -1:
            return False

        if self.memory_usage > self.memory_limit:
            logging.debug('culling: %s %s' % (
                            self.memory_usage, self.memory_limit))
            # use the key list in the logging dict to get oldest
            # drop to below 90% usage
            memory_limit = self.memory_limit - (.1 * self.memory_limit)
            removed = []
            while self.memory_usage > memory_limit:
                oldest_key = self.value_sizes.key_list[0]
                # remove the key from the memory store
                del self.memory_store[oldest_key]
                # remove it's size from the memory usage
                self.memory_usage -= self.value_sizes.get(oldest_key)
                # now remove it from the value sizes
                del self.value_sizes[oldest_key]
                removed.append(oldest_key)
            logging.debug('culled: %s' % ','.join(removed))
            return True
        return False


class MemoryMemcachedPlugin(MemcachedPlugin,MemoryStore):
    memory_store = {}

    def __init__(self,*args,**kwargs):
        # i think i can't use super here .. ?
        MemoryStore.__init__(self,kwargs.get('memory_limit'))
        MemcachedPlugin.__init__(self)

    def _set_data(self, key, value):
        super(MemoryMemcachedPlugin,self)._set_data(key,value)
        self.memory_store[key] = value
        self.value_sizes[key] = float(len(value))
        self.memory_usage += self.value_sizes[key]

        # not sure yet when i want to cull the memory pool,
        # lets try doing it every set?
        self.cull_pool()

        return True

    def _get_data(self, key):
        super(MemoryMemcachedPlugin,self)._get_data(key)
        return self.memory_store.get(key)

    def _delete_data(self, key):
        super(MemoryMemcachedPlugin,self)._delete_data(key)
        self.memory_usage -= self.value_sizes[key]
        del self.value_sizes[key]
        del self.memory_store[key]
        return True
