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
        try: # might not exist
            self.used_keys.remove(key)
        except KeyError:
            pass

    def _is_key_set(self,key):
        return key in self.used_keys

    def _set_data(self, key, value):
        logging.debug('setting data: %s %s' % (key,len(value)))
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
        logging.debug('handling get: %s' % keys)
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
        if self._is_key_set(key):
            data = self._get_data(key)
            self._delete_data(key)
            self.write_distinct_line('DELETED')
        else:
            self.write_distinct_line('NOT_FOUND')

        self.server.fire('memcached_delete',key,data)

    def __incr_data(self, key, value):
        # grab the existing data
        data = self._get_data(key)

        # if there's no data return False
        if not data:
            return False

        # make sure the value is a float
        value = float(value)

        # make sure the data is a float
        data = float(data)

        # if we have data lets apply our incr
        new_data = data + value

        # if the new value is less than 0 set it to 0
        if new_data < 0:
            new_data = 0

        # back to string
        new_data = str(new_data)

        # set our new data
        self._set_data(key,new_data)

        return True

    def _incr_data(self, key, value):
        self.__incr_data(key, value)

    def _decr_data(self, key, value):
        # use incr but w/ negative delta
        self.__incr_data(key, -value)

    def _handle_incr(self, key, value, noreply=False):
        # if the value is already in the response
        # than set it here. else do it and update
        # the value
        for line in self.response:
            if not line.startswith('NOT'):
                # looks like another plugin
                # as already taken the liberty
                # of incrementing for us!
                self._set_value(key,line[:-1])

        # increment the data
        if not self._incr_data(key, value):
            # if we couldn't do it return error
            self.write_distinct_line('NOT_FOUND')

        # return our new value if they want
        new_value = self._get_data(key)
        if not noreply:
            self.write_distinct_line('%s' % new_value)

        # let everyone else know we have incremented
        self.server.fire('memcached_incr',key,existing_value,value,new_value)

    def handle_incr(self, key, value, noreply=False):
        # pass through to the general function
        self._handle_incr(key, value)

    def handle_decr(self, key, value, noreply=False):
        # pass through to general, changing the sign so that
        # the value is decremented
        self._handle_incr(key, -value)

    
    # these are functions which will help
    # with setting / getting data w/o going
    # through the server
    def get_underhanded(self,key):
        """ will search through the memcache
            plugins looking for the value """
        for plugin in self.server.plugins:
            if isinstance(plugin,MemcachedPlugin) and not plugin is self:
                v = plugin._get_data(key)
                if v:
                    return v
        return None

    def set_underhanded(self,key,v):
        """
        Will set the value in all the memcache plugins
        """
        for plugin in self.server.plugins:
            if isinstance(plugin,MemcachedPlugin) and not plugin is self:
                plugin._set_data(key,v)
        return None

    def delete_underhanded(self,key):
        """
        Delete the value in al lthe memcache plugins
        """
        for plugin in self.server.plugins:
            if isinstance(plugin,MemcachedPlugin) and not plugin is self:
                plugin._delete_data(key)
        return None

    def incr_underhanded(self, key, value):
        """
        incr hte value in all the memcache plugins
        """
        for plugin in self.server.plugins:
            if isinstance(plugin,MemcachedPlugin) and not plugin is self:
                plugin._incr_data(key,value)
        return None

        

    # this property will return back a new client
    # for the running server
    # TODO: not have to use real sockets?
    def _get_server_client(self):
        host = self.server.host
        port = self.server.port
        # shit, this is probably going to block, killing the server
        c = memcache.Client(['%s:%s' % (host,port)])
        return c

    server_client = property(_get_server_client)
