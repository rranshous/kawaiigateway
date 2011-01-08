from base import Plugin

# we are going to store k/v to the disk
from diskdb import SimpleBlip as Blip
from memcached_plugin import MemcachedPlugin
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
        if value:
            blip.value = value
        return blip

    def _is_key_set(self, key):
        blip = self._get_blip(key)
        return blip.has_value()

    def _set_data(self, key, value):
        blip = self._get_blip(key,value)
        blip.flush()
        return True

    def _get_data(self, key):
        blip = self._get_blip(key)
        return blip.get_value()

    def _delete_data(self, key):
        blip = self._get_blip(key)
        blip.delete()
        return True

