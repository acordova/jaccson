
import sys
sys.path.append('../gen-py')

from jaccson import TableCursorService
from jaccson.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import json


class Connection(object):

    def __init__(self, hostport):
        try:
            host,portstring = hostport.split(':')
            port = int(portstring)
        except:
            host = hostport
            port = 27017

        # Make socket
        self.transport = TSocket.TSocket(host, port)
    
        # Buffering is critical. Raw sockets are very slow
        self.transport = TTransport.TBufferedTransport(self.transport)
    
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    
        # Create a client to use the protocol encoder
        self.proxy = TableCursorService.Client(protocol)
    
        # Connect!
        self.transport.open()

    def close(self):
        # Close!
        self.transport.close()

    def __getattribute__(self, attr):
        if not attr == 'close':
            # create a collection
            return Collection(attr, self.proxy)

        else:
            return self.close


class Collection(object):

    def __init__(self, name, proxy):
        self.name = name
        self.proxy = proxy

    def insert(self, jo):
        jos = json.dumps(jo)
        self.proxy.insert(self.name, jos)

    def get(self, oid):
        return self.proxy.get(self.name, oid)

    def findOne(self, query, select):
        return self.proxy.findOne(self.name, query, select)

    def find(self, query, select):
        label = self.proxy.find(self,name, query, select)
        return Cursor(label, self.proxy)

    def remove(self, query):
        self.proxy.remove(self.name, query)
                      
    def flush(self):
        self.proxy.flush(self.name)


class Cursor(object):

    def __init__(self, label, conn):
        self.label = label
        self.conn = conn
        self.batch = []
        self.done = False
    
    def next(self):
        
        if done:
            raise StopIteration()

        if len(self.batch) == 0:
            self.batch = self.conn.nextBatch(self.label)
            
            if len(self.batch) == 0:
                self.done = True
                raise StopIteration()

        s = self.batch[0]
        del self.batch[0]

        jo = json.loads(s)
        return jo

