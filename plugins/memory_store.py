from base import Plugin
from memcached_plugin import MemcachedPlugin
from ordered_dict import OrderedDict
import logging

class MemoryStore(object):
    memory_store = OrderedDict()
    def __init__(self,memory_limit=None):

        # sum of value sizes, bytes
        self.memory_usage = 0

        # limit of memory pool values. passed in as MB
        # but stored as bytes to compare against the usage
        self.memory_limit = memory_limit * 1048576 if memory_limit else -1
        logging.debug('memory_limit: %s' % self.memory_limit)

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
                assert len(self.memory_store), 'WTF, memory store len == 0'
                oldest_key = self.memory_store.iterkeys().next()
                self.memory_usage -= len(self.memory_store[oldest_key])
                del self.memory_store[oldest_key]
                removed.append(oldest_key)
            logging.debug('culled: %s' % ','.join(removed))
            return True
        return False


class MemoryMemcachedPlugin(MemcachedPlugin,MemoryStore):
    memory_store = {}
    cmdline_options = [
        (('memory_limit',),{'help':'MB caching max, -1 = no limit',
                            'default':100,'type':int})
    ]

    def __init__(self,*args,**kwargs):
        # i think i can't use super here .. ?
        MemoryStore.__init__(self,kwargs.get('memory_limit'))
        MemcachedPlugin.__init__(self)

    def _set_data(self, key, value):
        super(MemoryMemcachedPlugin,self)._set_data(key,value)
        self.memory_store[key] = value
        self.memory_usage += len(value)

        # not sure yet when i want to cull the memory pool,
        # lets try doing it every set?
        # TODO: better
        self.cull_pool()

        return True

    def _get_data(self, key):
        super(MemoryMemcachedPlugin,self)._get_data(key)
        return self.memory_store.get(key)

    def _delete_data(self, key):
        super(MemoryMemcachedPlugin,self)._delete_data(key)
        try:
            self.memory_usage -= len(self.memory_store[key])
            del self.memory_store[key]
        except KeyError:
            pass # not there =/
        return True
