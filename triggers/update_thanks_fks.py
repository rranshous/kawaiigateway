"""
Trigger for updating FK's on sets / deletes
""" 
from base import Trigger
import logging

class ThanksFKTrigger(Trigger):
    
    listeners = [
        ('memcached_set','handle_set'),
        ('memcached_delete','handle_delete')
    ]

    def _get_value_data(self,value):
        return json.loads(value)

    # TODO: don't just use code copied from
    # the memcached plugin
    def get_underhanded(self,key):
        """ will search through the memcache
            plugins looking for the value """
        for plugin in self.server.plugins:
            if isintance(plugin,MemcachedPlugin) and not plugin is self:
                v = plugin._get_data(key)
                if v:
                    return v
        return None

    # TODO: see above
    def set_underhanded(self,key,v):
        """
        Will set the value in all the memcache plugins
        """
        for plugin in self.server.plugins:
            if isintance(plugin,MemcachedPlugin) and not plugin is self:
                plugin._set_data(key,v)
        return None

    def handle_set(self,key,value):

        # see if they are setting something we care about
        if not key.startswith('/gift'):
            return False

        # un-json the value
        value_data = self._get_value_data(value)

        # get the event hash
        event_hash = value_data.get('_event_hash')

        # get the event's data
        event_data = self._get_value_data(
                        self.get_underhanded('/event/%s' % event_hash))

        # update the event's list of gifts
        gift_hash = value_data.get('_hash')
        event_data.setdefault('_gift_hashes',[]).append(gift_hash)

        # set the data again
        self.set_underhanded('/event/%s' % event_hash,event_data)

        return True

    def handle_delete(self,key,value):
        
        # see if it's a gift
        if not key.startswith('/gift'):
            return False

        # figure out what the event's hash is
        value_data = self._get_value_data(value)
        event_hash = value_data.get('_event_hash')

        # get the event's data
        event_data = self._get_data_value(
                           self.get_underhanded('/event/%s' % event_hash))

        # remove the gift from the event's gift list
        event_data.remove(value_data.get('_hash'))

        # set the data back
        self.set_underhanded('/event/%s' % event_hash,event_data)

        return True
