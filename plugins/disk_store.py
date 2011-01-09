from base import Plugin

# we are going to store k/v to the disk
from diskdb import SimpleBlip as Blip
from memcached_plugin import MemcachedPlugin
from memory_store import MemoryStore
import logging

class DiskMemcachedPlugin(MemcachedPlugin):
    cmdline_options = [
        (('storage_root',),
         {'default':None,
          'help':"where disk memcache plugin should save it's data"})
    ]

    def __init__(self,storage_root=None):
        super(DiskMemcachedPlugin,self).__init__()
        self.storage_root = storage_root

    def handle(self,stream,line,response):
        # if we don't know where my data is than don't do anything
        if self.storage_root:
            return super(DiskMemcachedPlugin,self).handle(stream,line,
                                                          response)
        else:
            logging.debug('no storage root set')
            response.append('END') # nothing to see here

        return True

    def _get_blip(self, key=None, value=None):
        logging.debug('getting blip: %s %s %s'
                      % (self.storage_root,key,len(value or '')))
        blip = Blip(self.storage_root,key=key)
        if value is not None:
            blip.set_value(value)
        return blip

    def _is_key_set(self, key):
        blip = self._get_blip(key)
        return blip.has_value()

    def _set_data(self, key, value):
        super(DiskMemcachedPlugin,self)._set_data(key,value)
        blip = self._get_blip(key,value)
        blip.flush()
        return True

    def _get_data(self, key):
        super(DiskMemcachedPlugin,self)._get_data(key)
        blip = self._get_blip(key)
        value = blip.get_value()

        # update the server's memory plugin w/ this
        # k/v pair since it must have missed
        self.update_memory_plugin(key,value)

        return value


    def _delete_data(self, key):
        super(DiskMemcachedPlugin,self)._delete_data(key)
        blip = self._get_blip(key)
        blip.delete()
        return True

    def update_memory_plugin(self,k,v):
        """
        Sets our k/v pair in the memory plugins if there are any
        """
        if self.server.plugins:
            for plugin in self.server.plugins:
                if isinstance(plugin,MemoryStore):
                    logging.debug('updating memory plugin: %s' % k)
                    plugin._set_data(k,v)

