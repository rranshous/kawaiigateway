"""
Trigger which will backfill keys / values
from the disk when the server starts
"""

from diskdb.utils import KeyManager
from base import Trigger

import logging

class DiskBackFill(Trigger):
    """
    Trigger to fill in memcache plugins from disk
    should be instantiated from the diskdb plugin
    so that the root can be set
    """

    listeners = [
        ('server_before_start','backfill')
    ]

    def __init__(self,data_root):
        self.data_root = data_root
        assert self.data_root, 'must provide storage root'
        super(DiskBackFill,self).__init__()

    def backfill(self,*args,**kwargs):
        from plugins.memcached_plugin import MemcachedPlugin
        from plugins.disk_store import DiskMemcachedPlugin, Blip

        logging.info('backfilling from disk')

        # get the keys existing on the drive
        keys = KeyManager.find_keys(self.data_root)

        # now we need to go through each of the keys
        # setting the key and value for any non-disk
        # memcache plugins
        for key in keys:
            blip = Blip(self.data_root,key=key)
            value = blip.get_value()
            if value is None:
                continue # don't need nothings
            for plugin in self.server.plugins:
                if isinstance(plugin,MemcachedPlugin) \
                   and not isinstance(plugin,DiskMemcachedPlugin):
                    plugin._set_data(key,value)
