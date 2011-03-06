"""
Base class for objects reacting to events
"""

import logging

class Trigger(object):
    def __init__(self):
        self.__server = None
        self.active_listeners = None

    # we are using a property for the server
    # so that when it is set or changed we
    # can add and remove our event listeners
    def _set_server(self,server):
        self.remove_listeners()
        self.__server = server
        self.add_listeners()

    def _get_server(self):
        return self.__server

    server = property(_get_server,
                      _set_server)


    def add_listeners(self):
        # loop through the listener defs adding them
        for event, action in self.listeners:
            logging.debug('adding listener: %s' % event)
            self.server.on(event,getattr(self,action))
            self.active_listeners.append((event,getattr(self,action)))

    def remove_listeners(self):
        # go through the handlers we've added removing them
        if self.server:
            for event, action in self.listeners:
                self.server.un(event,action)

        self.active_listeners = []
