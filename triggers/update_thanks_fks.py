"""
Trigger for updating FK's on sets / deletes
""" 
from base import Trigger
import logging
from plugins.memcached_plugin import MemcachedPlugin

try:
    import json
except ImportException:
    import jsonify as json

## TODO: rewrite this in a generic manner
##       so that random NS's can take advantage

class ThanksFKTrigger(Trigger):
    
    listeners = [
        ('memcached_set','handle_set'),
        ('memcached_delete','handle_delete')
    ]

    def __init__(self):
        super(ThanksFKTrigger,self).__init__()

        # obj types we are tracking FK's for
        self.obj_types = ['event','gift','user']

    def _deserialize_value_data(self,value):
        return json.loads(value)

    def _serialize_value_data(self,value):
        return json.dumps(value)

    # TODO: don't just use code copied from
    # the memcached plugin
    def get_underhanded(self,key):
        """ will search through the memcache
            plugins looking for the value """
        logging.debug('getting underhanded: %s' % key)
        for plugin in self.server.plugins:
            if isinstance(plugin,MemcachedPlugin) and not plugin is self:
                v = plugin._get_data(key)
                if v:
                    logging.debug('got underhanded: %s' % v)
                    return v
        return None

    # TODO: see above
    def set_underhanded(self,key,v):
        """
        Will set the value in all the memcache plugins
        """
        logging.debug('setting underhanded: %s %s' % (key,v))
        for plugin in self.server.plugins:
            if isinstance(plugin,MemcachedPlugin) and not plugin is self:
                plugin._set_data(key,v)
        return None

    def _remove_hash_from_other_data(self,obj_type,obj_hash,data):
        """
        remove the obj's hash from the other obj's list of hashes
        """

        # what's our list key?
        key = '_%s_hashes' % obj_type

        # remove our hash if it's there
        try:
            data.get(key,[]).remove(obj_hash)
        except ValueError:
            return False

        return True

    def _add_hash_to_other_data(self,obj_type,obj_hash,data):
        """
        append the obj's hash to the other obj's list of hashes
        """

        # what list are we appending to
        key = '_%s_hashes' % obj_type

        # add our obj's hash if it's not already there
        if obj_hash not in data.get(key,[]):
            data.setdefault(key,[]).append(obj_hash)

        # not strickly necissary
        return data

    def _iter_other_obj_refs(self,data):
        for k,v in data.iteritems():
            if k.startswith('_') and k.endswith('_hash'):
                if not k == '_hash':
                    yield (k,v)

    def obj_type(self,key):
        """ see if we care about this key """
        # see if they are setting something we care about
        # check the obj type.
        obj_type = [x for x in key.split('/') if x][0]
        if obj_type not in self.obj_types:
            return False
        return obj_type

    def handle_set(self,key,value):
        """
        if the name space matches one we are looking for we
        are going to look @ the data being set and if there
        are any root lvl keys which are `_(obj_type)_hash`
        than we are going to find the obj it's refering
        to and update it's `_(obj_type)_hashes` list to
        include the hash being set (if it's not already there)
        """

        obj_type = self.obj_type(key)
        if not obj_type:
            return False

        # get to the data obj
        value_data = self._deserialize_value_data(value)

        # look for root lvl key's in that are ref's to
        # other objects
        for k,v in self._iter_other_obj_refs(value_data):
            # get the other data
            other_obj_type = k[1:-5]
            other_obj_key = '/%s/%s' % (other_obj_type,v)
            logging.debug('other obj key: %s' % other_obj_key)
            other_data = self.get_underhanded(other_obj_key)
            if not other_data:
                continue
            other_data = self._deserialize_value_data(other_data)

            # add our key's hash to the fk hash list
            # of the other data
            self._add_hash_to_other_data(obj_type,
                                         value_data.get('_hash'),
                                         other_data)

            # push the other data back
            other_data = self._serialize_value_data(other_data)
            self.set_underhanded(other_obj_key,other_data)

            logging.debug('updated fk: %s %s' % (obj_type,k))

        return True

    def handle_delete(self,key,value):
        """
        removes FK hash refs if they exist
        """
        
        # see if we care
        obj_type = self.obj_type(key)
        if obj_type:
            return False

        # get the obj's data
        value_data = self._deserialize_value_data(value)

        # look for root lvl key's in that are ref's to
        # other objects
        for k,v in self._iter_other_obj_refs(value_data):
            # get the other data
            other_obj_type = k[1:-5]
            other_obj_key = '/%s/%s' % (other_obj_type,v)
            other_data = self.get_underhanded(other_obj_key)
            if not other_data:
                continue
            other_data = self._deserialize_value_data(other_data)

            # remove our key's hash to the fk hash list
            # of the other data
            self._remove_hash_from_other_data(obj_type,
                                              v, other_data)

            # push the other data back
            other_data = self._serialize_value_data(other_data)
            self.set_underhanded(other_obj_key,other_data)

            logging.debug('updated fk: %s %s' % (obj_type,k))

        return True
