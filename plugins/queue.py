"""
simple message queue based on memcache
"""

from base import Plugin

from memcached_plugin import MemcachedPlugin

from tornado.ioloop import IOLoop
import time

# simple way of keeping message info
Message = namedtuple('Message',['key','message','callback_token',
                                'temporary_key'])

class QueuePlugin(MemcachedPlugin):
    NS = 'queue'
    default_timeout = 60 # seconds

    """
    We are looking for set's which start w/ our NameSpace (NS).
    to add a new message you do a set to
    <NS>/<queue name>

    to do a delete you do the same but
    """

    def __init__(self,*args,**kwargs):
        # our lookup for messages
        # which have been pulled but not
        # deleted
        self.to_delete = {}

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
            return (None,[]) # guess not

        # did we have a namespace in the key ?
        name = p[1 if self.NS else 0]
        try:
            args = tuple(p[2 if self.NS else 1])
        except IndexException:
            args = []
    
        return (name,args)

    def _set_data(self, key, value):
        """ adds a message to the queue """
        name, args = self.parse_key(key)

        # you talkin to me?
        if not name: return False

        # check and see what the next ID in the queue is
        next = self.server_client.incr('%s/%s/head' % (self.NS,
                                                       name)

        # now update it to be our message
        self.server_client.set('%s/%s/%s' % (self.NS,
                                             info.get('name'),
                                             next))

        return True

    def _delete_data(self, key):
        """
        will keep a message from being added back into the
        queue after it's timeout. is ignored if there is
        no timeout pending
        """
        
        name, args = self.parse_key(key)

        # the first arg should be the sha of the message
        _hash = args[0]

        # now delete that queue message if it's still around
        message_details = self.to_delete.get(_hash)

        if not message_details:
            return True

        # grab our io loop
        loop = IOLoop.instance()

        # remove our re-add handler from the loop
        try:
            loop.remove_timeout(message_details.callback_token)
        except ValueError:
            pass # nothing to remove

        # remove the temporary message
        self.delete_underhanded(message_details.temporary_key)

        # and we're good 
        return True



    def _get_data(self, key):
        """ returns the next message.
            first arg is going to be timeout.
            a timeout of -1 mean don't track,
            a timeout of 0 mean we're just peaking
            so don't coun't this get. any other positive
            number is the number of seconds w/in a
            delete needs to be issued or the message
            will go back to the front of the queue """

        name, args = self.parse_key(key)

        # for us?
        if not name: return True

        # get the next key
        key = self.get_next_key()

        # grab it's data w/o going through the stack
        m = self.get_underhanded(key)

        # see if there is a timeout
        timeout = self.default_timeout
        if args:
            timeout = int(args[0])

        # if timeout is positive set it
        if timeout > -1:
            self.add_delete_watcher(key,m,timeout)

        # now lets remove the message
        self.delete_underhanded(key)

        # give back the message body
        return m

    def get_next_key(self):
        # TODO: make this not horribly terrible
        
        # sort our keys
        NS_keys = {}
        numbers = []
        for k in self.used_keys:
            name, args = self.parse_key(k)
            if name:
                # grab it's #
                numbers.append(int(args[0]))
                # add it to our key lookup
                NS_keys[numbers[-1]] = k

        # sort'm
        numbers.sort()

        # return the full key path
        return NS_keys[numbers[0]]


    def handle_not_deleted(self,_hash):
        """
        Uses the hash of the message to remove the message
        from limbo and add it to the begiing of the queue
        """
        pass # TODO

    def add_delete_watcher(self,key,m,timeout):
        """
        adds a task to the loop to execute in +timeout seconds
        re-adding the message to the queue if it has not been
        deleted
        """
        # try to use tornadio loop
        # to mark a message as removed from the message queue
        # but not yet deleted we are going to move it from it's
        # current key to NS/Qname/HASH
        # HASH being the sha of the msg

        message_hash = self._get_message_hash(m)
        name, args = self.parse_key(key)

        # create our new key
        new_key = '%s/'%self.NS if self.NS else ''
        new_key += '%s/%s' % (name,message_hash)

        # add our message under it's new key
        self.set_underhanded(new_key,m)

        # get the message hash, which is used
        # to track the message while it's in limbo
        _hash = self._get_sha(m)

        # get the current loop instance
        loop = IOLoop.instance()

        # setup our callback
        handle_not_deleted = self.handle_not_deleted
        def callback():
            handle_not_deleted(_hash)
        
        # add in our callback w/ the specified timeout
        callback_token = loop.add_timeout(time.time()+timeout,callback)
            
        # we need to add this message to our to_delete lookup
        self.to_delete[_hash] = Message(key,message,callback_token,new_key)

        # add we're done!
        return True

