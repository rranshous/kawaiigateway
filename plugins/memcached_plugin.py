from base import Plugin
import logging

####################################
# ty to github.com/superisaac/redqueue
####################################

class MemcachedPlugin(Plugin):
    def __init__(self):
        self.used_keys = set()

    def _key_set(self,key):
        self.used_keys.add(key)

    def _key_deleted(self,key):
        self.used_keys.remove(key)

    def _is_key_set(self,key):
        return key in self.used_keys

    def _set_data(self, key, value):
        self._key_set(key)
        return True

    def handle_set(self, key, flags, exptime, bytes, *args):
        # TODO: handle exptime
        bytes = int(bytes)
        exptime = int(exptime)
        if exptime > 0:
            exptime = time.time() + exptime
        set_data = self._set_data

        server = self.server
        def on_set_data(stream,line,response):
            data = line[:-2] # the last two chars are returns
            set_data(key,data)
            if 'STORED' not in response:
                response.append('STORED')
            server.fire('memcached_set',key,data)
            
        return on_set_data

    def _get_data(self, key):
        return True

    def handle_get(self, *keys):
        for key in keys:
            # since another plugin could have already
            # supplied the value lets check
            if len([x for x in self.response
                               if x.startswith('VALUE %s' % key)]) > 0:
                logging.debug('skipping value response')
                continue
            if self._is_key_set(key):
                data  = self._get_data(key)
                if data:
                    self.write_distinct_line('VALUE %s 0 %d'
                                             % (key,len(data)))
                    self.write_distinct_line(data)
                self.server.fire('memecached_get',key,data)
        self.write_distinct_line('END')

    def _delete_data(self, key):
        self._key_deleted(key)
        return True

    def handle_delete(self, key, *args):
        if key in self.used_keys:
            data = self._get_data(key)
            self._delete_data(key)
            self.write_distinct_line('DELETED')
        else:
            self.write_distinct_line('NOT_FOUND')

        self.server.fire('memcached_delete',key,data)

