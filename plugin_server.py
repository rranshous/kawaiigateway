import logging
import socket
from tornado import ioloop
from tornado import iostream
from bigsignal import Eventable

class PluginServer(Eventable):
    def __init__(self,plugins=[],triggers=[]):
        super(PluginServer,self).__init__()

        logging.debug('plugin server init')

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

    @classmethod
    def get_handle_read(cls,stream,plugins,handlers=[]):
        logging.debug('plugin server getting handle read:: handlers: %s'
                      % handlers)

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

            # wait for the next line
            logging.debug('callables: %s' % callables)
            cls.wait_for_line(stream,
                              cls.get_handle_read(stream,plugins,
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
        self.fire('server_start',self)

