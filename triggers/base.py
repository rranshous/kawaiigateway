"""
Base class for objects reacting to events
"""

class Trigger(object):
    def __init__(self):
        self.__server = None
        self.active_listeners = None

    # we are using a property for the server
    # so that when it is set or changed we
    # can add and remove our event listeners
    def _set_server(self,server):
        self.deactivate_listeners()
        self.__server = server
        self.add_listeners()

    def _get_server(self):
        return self.__server

    server = property(self._set_server,
                      self._get_server)


