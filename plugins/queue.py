"""
simple message queue based on memcache
"""

from base import Plugin

from memcached_plugin import MemcachedPlugin

from tornado.ioloop import IOLoop
import time

import logging

from collections import namedtuple

import hashlib

try:
    import json
except ImportException:
    import jsonify as json

# simple way of keeping message info
Message = namedtuple('Message',['key','message','callback_token',
                                'temporary_key'])

class QueuePlugin(MemcachedPlugin):
    # the namespace we're opperating in
    NS = 'queue'

    # sub name spaces we should not act on
    ignored_sub_namespaces = ['head','out']

    default_timeout = 60 # seconds

    """
    We are looking for set's which start w/ our NameSpace (NS).
    to add a new message you do a set to
    <NS>/<queue name>

    to do a delete you do the same but
    """

    def __init__(self):
        # our lookup for messages
        # which have been pulled but not
        # deleted
        self.to_delete = {}

        logging.debug('init queue')
        
        # pay ur respects
        super(QueuePlugin,self).__init__()

    def _get_md5(self,s):
        md5 = hashlib.md5()
        md5.update(s)
        _hash = md5.hexdigest()
        return _hash

    def _is_key_set(self,key):
        """
        this function acts as the gatekeeper. before getting
        to our plugins data handlers it's going to check and see
        if we have the key. Since we won't actually have a key set for
        the things it's setting we need to pretend we do.
        """
        # since none of our functions want someting outside
        # our name space filter for that
        name, args = self.parse_key(key)

        if not name:
            return False

        # TODO now we can actually take a look and see
        return True

    def _get_next_head(self, name):
        """
        returns the next head id for the queue name
        """

        # we are going to increment our head counter
        head_key = '%s/%s/head' % (self.NS,name)
        if not self.incr_underhanded(head_key,'1'):
            # no head exists, start one
            self.set_underhanded(head_key,'0')
            return '0'

        # and than get the new head value
        next = self.get_underhanded(head_key)

        # we want to return an integer
        # next should be a decimal
        next = str(next.split('.')[0])

        # and return that bitch
        return next

    def parse_key(self,k):
        """
        keys should be namespace, queuename, args*
        seperated by forward slash.
        we are going to return the a tuple with
        (name,[args])

        the returned tuple will always be len 2

        """
        p = [s for s in k.split('/') if s]
        info = {}
        
        # first see if the namespace matches
        if not self.NS or p[0] != self.NS:
            return (None,[]) # guess not

        # did we have a namespace in the key ?
        name = p[1 if self.NS else 0]
        try:
            args = tuple(p[2 if self.NS else 1:])
        except:
            args = []
    
        return (name,args)

    def _set_data(self, key, value):
        """ adds a message to the queue """

        # parse our key's info
        name, args = self.parse_key(key)

        logging.debug('wants me to set data: %s' % key)

        # if it's an 'out' sub name space than
        # we are re-adding in a currently 'out' message
        # aka one which should be re-added at some point
        # if we get one of those and it doesn't already
        # exist setup the data and the timed handler
        if args and args[0] == 'out':
            logging.debug('handling already out key')

            # no one else need touch this one
            self.server.skip_other_handlers = True

            # and we are done
            return self._set_out_data(key, value)

        # if it is a head request ignore it
        if args and args[0] in self.ignored_sub_namespaces:
            logging.debug('skipping %s' % key)
            return False

        # if we have args than we must be
        # doing something other than a normal set
        if args:
            logging.debug('super n stop: %s' % key)
            super(QueuePlugin,self)._set_data(key,value)
            return False

        logging.debug('_set_data: %s %s' % (name,args))

        # get our next head for the queue
        next = self._get_next_head(name)

        logging.debug('next head: %s' % next)

        # now update it to be our message
        new_key = '%s/%s/%s' % (self.NS,name,next)
        self.set_underhanded(new_key,value)

        # stop the other plugins from storing
        # queue/<name> values based on our return value
        logging.debug('setting skip other handlers')
        self.server.skip_other_handlers = True

        # show some respect to ur elders
        super(QueuePlugin,self)._set_data(new_key,value)

        return True

    def _set_out_data(self, key, value):
        # this method ends up losing the original
        # key and instead setting up a new 'out'
        # and 'in' keys

        # what if we get a set for an 'out' message?
        # such as from disk backfill
        name, args = self.parse_key(key)

        # see if we already exist
        if key in self.used_keys:
            return False

        # ok so we are adding it, but we dont know it's
        # original key so we don't know where it goes once
        # it's been re-added. lets add it to the end for now
        # TODO not just add to the end
        next_head = self._get_next_head(name)

        # set our msg @ the new head
        new_key = '%s/%s/%s' % (self.NS,name,next_head)
        self.set_underhanded(new_key,value)

        # now that it's set add our delete handler
        # for it as though it was immediately "got"
        self.add_delete_watcher(new_key,value,self.default_timeout)

        return True

    def _delete_data(self, key):
        """
        will keep a message from being added back into the
        queue after it's timeout. is ignored if there is
        no timeout pending
        """
        
        name, args = self.parse_key(key)

        # the first arg should be the md5 of the message
        _hash = args[0]

        # now delete that queue message if it's still around
        message_details = self.to_delete.get(_hash)

        # guess it's gone
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

        # remove the to_delete reference
        del self.to_delete[_hash]

        # show some respect to ur elders
        super(QueuePlugin,self)._delete_data(key)

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

        logging.debug('got key: %s' % key)

        name, args = self.parse_key(key)

        # get the next key
        key = self.get_next_key(name)

        # if we didn't get a next key than there 
        # are no items to be had
        if not key: return None

        logging.debug('next_key: %s' % key)

        # grab it's data w/o going through the stack
        m = self.get_underhanded(key)

        # if there isn't a mesasge to be had, fail
        if not m:
            logging.debug('empty queue')
            return False

        # make sure that our arg is actually not a
        # head request or something
        if args and args[0] in self.ignored_sub_namespaces:
            return None

        logging.debug('getting: %s' % name)

        # see if there is a timeout
        timeout = self.default_timeout
        if args:
            timeout = int(args[0])

        # if timeout is positive set it
        if timeout > 0:
            self.add_delete_watcher(key,m,timeout)

        # if the timeout is 0 than they only
        # want to peek the msg, not actually consume it

        # if timeout is < 0 we want to delete it immediately
        if timeout < 0:
            logging.debug('deleting key: %s' % key)

            # now lets remove the message
            self.delete_underhanded(key)

            # delete underhanded doesn't update us
            # we are only storing the key
            self._key_deleted(key)

        # stop the other plugins from storing
        # queue/<name> values based on our return value
        logging.debug('setting skip other handlers')
        self.server.skip_other_handlers = True

        # respect son !
        super(QueuePlugin,self)._get_data(key)

        # give back the message body
        return m

    def get_next_key(self,name):
        # TODO: make this not horribly terrible

        # sort our keys
        NS_keys = {}
        numbers = []
        for k in self.used_keys:
            used_name, args = self.parse_key(k)
            if used_name and used_name == name:
                # make sure it's not 'out' or something
                if args and args[0] in self.ignored_sub_namespaces:
                    continue

                # grab it's #
                numbers.append(int(args[0]))

                # add it to our key lookup
                NS_keys[numbers[-1]] = k

        # sort'm
        numbers.sort()

        # if we didn't find anything .. None it is
        if not NS_keys or not numbers:
            return None

        # return the full key path
        return NS_keys[numbers[0]]

    def handle_delete(self, key, *args):
        if self._is_key_set(key):
            # updated for parent to use underhanded
            data = self.get_underhanded(key)
            self._delete_data(key)
            # we don't care if someone else already
            # said not found
            self.response = []
            self.write_distinct_line('DELETED')
            self.server.fire('memcached_delete',key,data)
        else:
            self.write_distinct_line('NOT_FOUND')


    def handle_not_deleted(self,original_path,current_path):
        """

        Uses the hash of the message to remove the message
        from limbo and add it to the begiing of the queue
        """

        logging.debug('handling not deleted: %s %s' % (original_path,current_path))

        # grab the message
        m = self.get_underhanded(current_path)

        # push it to it's old key
        self.set_underhanded(original_path,m)

        # remove it from it's current key
        self.delete_underhanded(current_path)

        # remove the out key, add the original key to tracking
        self._key_deleted(current_path)
        self._key_set(original_path)

    def handle_stats(self,queue_name=None):
        """
        returns back stats about the queue
        including the current messages.
        If no queue is passed than data for
        all queue's is returned
        """

        in_messages = 0
        out_messages = 0

        # go through all our keys
        for key in self.used_keys:
            name, args = self.parse_key(key)

            # if there is a queue name defined
            # than limit the keys to that queue
            if not queue_name or name == queue_name:

                # grab the data for the key
                msg = self.get_underhanded(key)

                # if there's no data .. go on
                if not msg:
                    continue

                # write back our response for this key
                # the response key will either be the number (in)
                # or hash (if out)
                is_out = True if args and args[0] == 'out' else False
                response_key = args[1] if is_out else args[0]
                self.write_line('STAT %s/%s %s' % (name,
                                                   response_key,
                                                   msg))

                # increment our msg counters
                if is_out:
                    out_messages += 1
                else:
                    in_messages += 1


        # HERE we'd output other stats, for now
        # just the messages are good enough
        
        # and we're done
        self.write_line('END')

        return True

    def add_delete_watcher(self,key,m,timeout):
        """
        adds a task to the loop to execute in +timeout seconds
        re-adding the message to the queue if it has not been
        deleted
        """
        logging.debug('adding delete watcher: key: %s ;; message: %s ;; timeout: %s'
                        % (key,m,timeout))

        # try to use tornadio loop
        # to mark a message as removed from the message queue
        # but not yet deleted we are going to move it from it's
        # current key to NS/Qname/HASH
        # HASH being the md5 of the msg

        # get the hash of the msg, used to track it
        # while it's in 'out' limbo
        _hash = self._get_md5(m)
        name, args = self.parse_key(key)

        # create our new key
        new_key = '%s/'%self.NS if self.NS else ''
        new_key += '%s/out/%s' % (name,_hash)

        # add our message under it's new key
        self.set_underhanded(new_key,m)

        # update that we are tracking it
        self._key_set(new_key)

        # get the current loop instance
        loop = IOLoop.instance()

        # setup our callback
        handle_not_deleted = self.handle_not_deleted
        def callback():
            handle_not_deleted(key,new_key)
        
        # add in our callback w/ the specified timeout
        callback_token = loop.add_timeout(time.time()+timeout,callback)
            
        # we need to add this message to our to_delete lookup
        self.to_delete[_hash] = Message(key,m,callback_token,new_key)

        # remove the current data
        self.delete_underhanded(key)

        # remove the fact we are tracking it
        self._key_deleted(key)

        # add we're done!
        return True

