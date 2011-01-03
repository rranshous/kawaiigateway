#!/usr/bin/python

####################################################################
#
# code gratefully inspired by github.com/superisaac/redqueue
#
####################################################################

import re, os, sys
import logging

from tornado import ioloop
import tornado.options
from tornado.options import define, options
from redqueue.server import Server
from redqueue import task
import plugins

define('host', default="0.0.0.0", help="The binded ip host")
define('port', default=11211, type=int, help='The port to be listened')
define('logfile', default='',
                  help='Place where logging rows(info, debug, ...) are put.')


def PluginServer(object):
    def __init__(self,plugins,memory_store):
        # plugins is a list of objects w/ a callable handle
        self.plugins = plugins

        # memory store shared between all connections
        self.memory_store = memory_store

    def handle_accept(self, fd, events):
        # we've got a new connection
        conn, addr = self._sock.accept()
        stream = iostream.IOStream(conn)
        # wait for some data to come down the line
        self.wait_for_line(stream,self.get_handle_read(stream,
                                                       self.plugins))

    @classmethod
    def wait_for_line(cls,stream,func):
        # read from the stream until we hit a new line
        stream.read_until('\r\n', func)

    @classmethod
    def get_handle_read(cls,stream,plugins,handlers=[]):
        # if we weren't passed any handlers than use the
        # plugin handlers
        if not handlers:
            handlers = [getattr(p,'handle') for p in plugins]

        def handle_read(cls,line):
            # pass our stream, line, memory store, and response
            # to each of the plugin handles
            response = None
            callables = []
            for func in handlers:
                # if the handle function returns a callable
                # than we should wait on the stream for the next
                # line and pass it to the callable
                r = plugin.handle(stream,line,self.memory_store)
                if callable(r):
                    callables.append(r)

            # wait for the next line
            cls.wait_for_line(cls.get_handle_read(stream,plugins,
                                                  handlers=callables))

    def start(self, host, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host,port))
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)

def main():
    # we need to give our plugins a chance
    # to define cmdline options
    for plugin in plugins.plugins:
        for option in getattr(plugin.cmdline_options,[]):
            define(*option[0],**option[1])

    # parse the command line
    tornado.options.parse_command_line()

    # initialize the plugins
    # init the plugins passing them (as named args)
    # the result of their command line options
    active_plugins = []
    for plugin in plugins.plugins:
        kwargs = dict([ (o[0][0],None) for o in plugin.cmdline_options  ])
        for k in kwargs.keys():
            kwargs[k] = getattr(options,k)

    # enable logging possibly
    if options.logfile:
        logging.basicConfig(filename=options.logfile, level=logging.DEBUG)

    # start up our server
    memory_store = {}
    server = Server(plugins,memory_store)
    server.start(options.host, options.port)
    task.run_all(server)
    ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()

