from base import Plugin

####################################
# ty to github.com/superisaac/redqueue
####################################

# we are going to do simple k/v storage in the memory pool
class MemcachedMemoryStore(Plugin):

    def __init__(self):
        self.steram = None
        self.protocol_id = str(id(self))
        self.used_keys = set()

    def handle_set(self, key, flags, exptime, bytes, *args):
        bytes = int(bytes)
        exptime = int(exptime)
        if exptime > 0:
            exptime = time.time() + exptime
        def on_set_data(stream,line,response,memory_store):
            data = line[:-2] # the last two chars are returns
            self.memory_store[key] = data
            self.stream.write('STORED\r\n')
        self.memory_store[key] = None
        return on_set_data

    def _get_data(self, key):
        return self.memory_store.get(key)

    def handle_get(self, *keys):
        for key in keys:
            data  = self._get_data(key)
            if data:
                self.stream.write('VALUE %s 0 %d\r\n%s\r\n'
                                  % (key,len(data), data))
        self.stream.write('END\r\n')

    def handle_gets(self, *keys):
        for key in keys:
            data = self._get_data(key)
            if data:
                self.stream.write('VALUE %s 0 %d\r\n%s\r\n'
                                  % (key, len(data), data))
                break
        self.stream.write('END\r\n')

    def handle_delete(self, key, *args):
        if key in self.used_keys:
            self.stream.write('DLETED\r\n')
            self.used_keys.remove(key)
        else:
            self.stream.write('NOT_DELETED\r\n')
