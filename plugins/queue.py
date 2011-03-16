"""
simple message queue based on memcache
"""

from base import Plugin

from memcached_plugin import MemcachedPlugin

class QueuePlugin(MemcachedPlugin):
    NS = 'queue'

    """
    We are looking for set's which start w/ our NameSpace (NS).
    to add a new message you do a set to
    <NS>/<queue name>

    to do a delete you do the same but
    """

    def parse_key(self,k):
        """
        keys should be namespace, queuename, args*
        seperated by forward slash.
        we are going to return the a tuple with
        (name,[args])

        the returned tuple will always be len 2

        """
        p = [s for s in key.split('/') if s]
        info = {}
        
        # first see if the namespace matches
        if not self.NS or p[0] =! self.NS:
            return (,) # guess not

        # did we have a namespace in the key ?
        name = p[1 if self.NS else 0]
        try:
            args = tuple(p[2 if self.NS else 1])
        except IndexException:
            args = []
    
        return (name,args)

    def _set_data(self, key, value):
        """ adds a message to the queue """
        info = self.parse_key(key)

        # you talkin to me?
        if not info: return False

        # check and see what the next ID in the queue is
        next = self.server_client.incr('%s/%s/head' % (self.NS,
                                                       info.get('name'))

        # now update it to be our message
        self.server_client.set('%s/%s/%s' % (self.NS,
                                             info.get('name'),
                                             next))

        return True

        
    def _get_data(self, key, value):
        """ returns the next message """
        info = self.parse_key(key)

        # for us?
        if not info: return {}

        # get the next key
        key = self.get_next_key()

    def get_next_key(self):
        # TODO: make this not horribly terrible
        
        # sort our keys
        NS_keys = {}
        numbers = []
        for k in self.used_keys:
            info = self.parse_key(k)
            if info:
                # grab it's #
                numbers.append(int(info[1][0]))
                # add it to our key lookup
                NS_keys[numbers[-1]] = k

        # sort'm
        numbers.sort()

        # return the full key path
        return NS_keys[numbers[0]]


