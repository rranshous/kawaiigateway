"""
Base plugin object for handling streams
via tornado
"""

class Plugin(object):
    # if you want args from the cmd line than
    # set them here, you are pssing up args
    # for the tornado define method for cmdline options
    # ex: = [((,),{}),..]
    # these will get passed to the object when it is initialized
    cmdline_options = []

    def handle(self,stream,line,response,memory_store):

        self.stream = stream
        self.line = line
        self.response = response
        self.memory_store = memory_store

        # simple default handler which will break
        # the string down into <action> arg arg arg ..
        # and than pass the args to a handler method
        # if it finds one named handle_<action>

        action = line.split()[0]
        args = line.split()[1:]
        method_name = 'handle_%s' % action.lower()
        if callable(getattr(self,method_name,False)):
            return getattr(self,method_name)(*args)

        return False
