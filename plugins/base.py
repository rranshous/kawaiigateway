"""
Base plugin object for handling streams
via tornado
"""
import logging

class Plugin(object):
    # if you want args from the cmd line than
    # set them here, you are pssing up args
    # for the tornado define method for cmdline options
    # ex: = [((,),{}),..]
    # these will get passed to the object when it is initialized
    # cmdline_options = []

    cmdline_options = []

    # init expects kwargs for each of the cmdline options
    def __init__(self):
        # the server will set the server attribute
        # when the plugin is being used by a server
        self.server = None

    def handle(self,stream,line,response):

        self.stream = stream
        self.line = line
        self.response = response

        # simple default handler which will break
        # the string down into <action> arg arg arg ..
        # and than pass the args to a handler method
        # if it finds one named handle_<action>

        action = line.split()[0]
        args = line.split()[1:]
        method_name = 'handle_%s' % action.lower()
        if callable(getattr(self,method_name,False)):
            logging.debug('plugin calling: %s %s' % (self.__class__.__name__,
                                                     method_name))
            return getattr(self,method_name)(*args)

        return False

    # convenience method for adding to the response list
    # write will append to the last line of the response
    def write(self,data):
        if not self.response:
            self.response.append('')
        self.response[-1] += str(data)

    # writes a new line to the response list
    def write_line(self,data):
        self.response.append(data)

    # if the given line is already in the response
    # remove it before appending the line
    def write_distinct_line(self,data):
        if data in self.response:
            self.response.remove(data)
        self.write_line(data)
