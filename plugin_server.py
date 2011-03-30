import logging
import socket
from tornado import ioloop
from tornado import iostream
from bigsignal import Eventable

class PluginServer(Eventable):
    def __init__(self,plugins=[],triggers=[]):
        super(PluginServer,self).__init__()

        logging.debug('plugin server init')

        # this flag tells other whether we should
        # skip the handlers left in a handle read loop
        # it gets reset False @ the begining of each handle read
        self.skip_other_handlers = False

        # flag for if we are currentling
        # firing our events
        self.firing = True
        # list of events to fire
        self.to_fire = []

        # who we serving?
        self.host = None
        self.port = None

        # plugins is a list of objects w/ a callable handle
        self.plugins = plugins
        
        # triggers is a list of triggers
        self.triggers = triggers

        # let the plugins know they are being used by a server
        for plugin in self.plugins:
            logging.debug('setting server on: %s' % plugin.__class__)
            plugin.server = self

        # let the triggers know
        for trigger in self.triggers:
            logging.debug('setting server on: %s:' % trigger.__class__)
            trigger.server = self

    def handle_accept(self, fd, events):
        logging.debug('plugin server handling accept')
        # we've got a new connection
        conn, addr = self._sock.accept()
        stream = iostream.IOStream(conn)

        # wait for some data to come down the line
        self.wait_for_line(stream,self.get_handle_read(stream,
                                                       self.plugins))

    @staticmethod
    def wait_for_line(stream,func):
        logging.debug('plugin server waiting for line')

        # read from the stream until we hit a new line
        stream.read_until('\r\n', func)

    def get_handle_read(self,stream,plugins,handlers=[]):
        logging.debug('plugin server getting handle read:: handlers: %s'
                      % handlers)

        # reset our flag
        self.skip_other_handlers = False

        # if we weren't passed any handlers than use the
        # plugin handlers
        if not handlers:
            logging.debug('no handlers, using plugins')
            handlers = [getattr(p,'handle') for p in plugins]

        def handle_read(line):
            logging.debug('handling read') #: %s' % line)
            # pass our stream, line, and response
            # to each of the plugin handles
            response = []
            callables = []
            for func in handlers:
                # if they want us to skip, do so
                if self.skip_other_handlers:
                    logging.debug('skipping other handlers')
                    continue

                # if the handle function returns a callable
                # than we should wait on the stream for the next
                # line and pass it to the callable
                logging.debug('calling func: %s' % func.__name__)
                r = func(stream,line,response)
                logging.debug('response: %s' % len(response))
                if callable(r):
                    callables.append(r)

            # now that all the handlers have had their chance
            # write the resonse out
            for line in response:
                for l in line.strip().split('\r\n'):
                    stream.write('%s\r\n' % l)

            # now that we've gone through all the handlers
            # for this line, fire the events we've been caching
            self.fire_events()

            # wait for the next line
            logging.debug('callables: %s' % callables)
            self.wait_for_line(stream,
                               self.get_handle_read(stream,plugins,
                                                    handlers=callables))

        return handle_read

    def start(self, host, port):
        # let those listening know we are about to begin
        self.fire('server_before_start',self)

        logging.debug('plugin server starting')
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host,port))
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)

        self.host = host
        self.port = port

        # it has begun
        # update the firing so this go out immediately
        self.fire('server_start',self)

        # we no longer want to fire immediately
        self.firing = False


    # we want to be an eventable but we want to delay firing the events
    # until the cycle is done. If more events fire once the cycle is complete
    # we do them all until they are done and than do the next cycle
    def fire(self,*args,**kwargs):
        # are we currently firing?
        if not self.firing:
            # keep our events to the side
            self.to_fire.append((args,kwargs))
        else:
            # if we are firing just let'm go
            Eventable.fire(self,*args,**kwargs)

    def fire_events(self):
        # now we actually fire them
        self.firing = True

        for args, kwargs in self.to_fire:
            Eventable.fire(self,*args,**kwargs)

        # reset our to fire
        self.to_fire = []

        # and we're done
        self.firing = False
