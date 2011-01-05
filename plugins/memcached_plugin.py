from base import Plugin

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
        return True

    def handle_set(self, key, flags, exptime, bytes, *args):
        # TODO: handle exptime
        bytes = int(bytes)
        exptime = int(exptime)
        if exptime > 0:
            exptime = time.time() + exptime
        set_data = self._set_data
        def on_set_data(stream,line,response):
            data = line[:-2] # the last two chars are returns
            set_data(key,data)
            if 'STORED' not in response:
                response.append('STORED')
        self._key_set(key)
        return on_set_data

    def _get_data(self, key):
        return True

    def handle_get(self, *keys):
        for key in keys:
            if self._is_key_set(key):
                data  = self._get_data(key)
                if data:
                    self.write_distinct_line('VALUE %s 0 %d'
                                             % (key,len(data)))
                    self.write_distinct_line(data)
        self.write_distinct_line('END')

    def _delete_data(self, key):
        return True

    def handle_delete(self, key, *args):
        if key in self.used_keys:
            self._delete_data(key)
            self.write_distinct_line('DELETED')
            self._key_deleted(key)
        else:
            self.write_distinct_line('NOT_FOUND')

