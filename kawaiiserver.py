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
import plugins
import triggers
from plugin_server import PluginServer as Server

define('host', default="0.0.0.0", help="The binded ip host")
define('port', default=11211, type=int, help='The port to be listened')

def main():


    # we need to give our plugins a chance
    # to define cmdline options
    logging.debug('plugins: %s' % plugins.plugins)
    for plugin in plugins.plugins:
        for args,kwargs in getattr(plugin,'cmdline_options',[]):
            logging.debug('defining cmdline arg: %s %s %s'
                          % (plugin,args,kwargs))
            define(*args,**kwargs)

    # parse the command line
    print 'parsing'
    tornado.options.parse_command_line()
    logging.debug('parsing cmd line')

    # initialize our triggers
    # triggers will react to events
    active_triggers = []
    logging.debug('initializing triggers')
    for trigger in triggers.triggers:
        # we are pushing the events through the server
        active_triggers.append(trigger())

    # initialize the plugins
    # init the plugins passing them (as named args)
    # the result of their command line options
    active_plugins = []
    logging.debug('initializing plugins')
    for plugin in plugins.plugins:
        kwargs = dict([ (o[0][0],None) for o in plugin.cmdline_options  ])
        for k in kwargs.keys():
            kwargs[k] = getattr(options,k)
        logging.debug('plugin: %s :: kwargs: %s' % (plugin,kwargs))
        active_plugins.append(plugin(**kwargs))

    # start up our server
    server = Server(active_plugins,active_triggers)
    server.start(options.host, options.port)

    # start the big loop in the sky
    ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()

