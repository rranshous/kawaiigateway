"""
we are going to store k/v to the disk
"""

from base import Plugin

from diskdb import SimpleBlip as Blip
from memcached_plugin import MemcachedPlugin
from memory_store import MemoryStore
import logging
from triggers import DiskBackFill
import multiprocessing, Queue

class DiskMemcachedPlugin(MemcachedPlugin):
    cmdline_options = [
        (('storage_root',),
         {'default':None,
          'help':"where disk memcache plugin should save it's data"}),
        (('seperate_process',),
         {'default':False,
          'help':"should we do our file writes in a seperate process?"}),
        (('backfill',),
         {'default':True,
          'help':"populate from the storage root"})
    ]

    def _set_server(self,server):
        """
        setup our trigger now that we have a server
        """
        self.__server = server
        logging.debug('disk cache setting server on backfill trigger')
        if getattr(self,'backfill_trigger',None):
            self.backfill_trigger.server = server

    server = property(lambda s: s.__server,
                      _set_server)

    def __init__(self,storage_root=None,
                      backfill=True,
                      seperate_process=False):
        super(DiskMemcachedPlugin,self).__init__()

        # massage our cmdline options
        # TODO move this out of the plugin
        if backfill in (True,'True','true','y','1'):
            backfill = True
        else:
            backfill = False
        if seperate_process in (True,'True','true','y','1'):
            seperate_process = True
        else:
            seperate_process = False

        # TODO: These cmd line options dont appear to be respected
        logging.debug("backfill: %s" % str(backfill))
        logging.debug("seperate_process: %s" % str(seperate_process))
        logging.debug("storage_root: %s" % storage_root)

        # where's the data going?
        self.storage_root = storage_root

        # if we can, lets add the back fill trigger
        # so that the other memcached plugins can get
        # to our persistant datas =]
        if backfill:
            self.backfill_trigger = DiskBackFill(self.storage_root)

        # we are going to setup a seperate process to do our
        # file writing in
        self.seperate_process = seperate_process
        if self.seperate_process:
            logging.debug('starting seperate writer process')
            self.write_queue = multiprocessing.Queue()
            self.write_process = WriterProcess(self.storage_root,
                                               self.write_queue)
            self.write_process.start()

    def handle(self,stream,line,response):
        # if we don't know where my data is than don't do anything
        if self.storage_root:
            return super(DiskMemcachedPlugin,self).handle(stream,line,
                                                          response)
        else:
            logging.debug('no storage root set')
            response.append('END') # nothing to see here

        return True

    def _get_blip(self, key=None, value=None):
        logging.debug('getting blip: %s %s %s'
                      % (self.storage_root,key,len(value or '')))
        blip = Blip(self.storage_root,key=key)
        if value is not None:
            blip.set_value(value)
        return blip

    def _is_key_set(self, key):
        blip = self._get_blip(key)
        return blip.has_value()

    def _set_data(self, key, value):
        # if we have a seperate process use it
        if self.seperate_process:
            logging.debug('disk set data using seperate process')
            self.write_queue.put((key,value))
        else:
            self._write_data(key,value)

        return True

    def _write_data(self,key,value):
        logging.debug('writing to disk %s' % key)
        super(DiskMemcachedPlugin,self)._set_data(key,value)
        blip = self._get_blip(key,value)
        blip.flush()

        return True

    def _get_data(self, key):
        super(DiskMemcachedPlugin,self)._get_data(key)
        blip = self._get_blip(key)
        value = blip.get_value()

        # update the server's memory plugin w/ this
        # k/v pair since it must have missed
        if value:
            self.update_memory_plugin(key,value)

        return value

    def _delete_data(self, key):
        super(DiskMemcachedPlugin,self)._delete_data(key)
        blip = self._get_blip(key)
        blip.delete()
        return True

    def update_memory_plugin(self,k,v):
        """
        Sets our k/v pair in the memory plugins if there are any
        """
        if self.server.plugins:
            for plugin in self.server.plugins:
                if isinstance(plugin,MemoryStore):
                    logging.debug('updating memory plugin: %s' % k)
                    plugin._set_data(k,v)



class WriterProcess(multiprocessing.Process):

    """
    writes to the disk outside the main process
    """

    def __init__(self,storage_root,work_queue):
        self.work_queue = work_queue
        self.storage_root = storage_root
        super(WriterProcess,self).__init__()

    def run(self):

        storage_root = self.storage_root
        work_queue = self.work_queue

        # create our own disk plugin
        # we'll lean on it to do the work
        disk_plugin = DiskMemcachedPlugin(storage_root,
                                          backfill=False,
                                          seperate_process=False)

        logging.debug('sub process running')

        # now just sit on our work queue
        while True:
            try:
                # we just wait and wait for work
                work = work_queue.get(True,5)

                logging.debug('process writing: %s %s' % work)

                # the work should be a tuple with the key and value
                disk_plugin._write_data(*work)
            except Queue.Empty:
                pass
