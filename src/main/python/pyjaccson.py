
import sys
sys.path.append('gen-py')

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

    def getCollection(self, cname):
        return Collection(cname, self)


class Collection(object):

    def __init__(self, name, conn):
        self.name = name
        self.proxy = conn.proxy
        self.batch = []
        self.batchSize = 100
        
    def _insertBatch(self):
        self.proxy.insertBatch(self.name, self.batch)
        self.proxy.flush(self.name)
        self.batch = []
        
    def insert(self, jo):
        jos = json.dumps(jo)
        self.batch.append(jos)
        if len(self.batch) > self.batchSize:
            self._insertBatch()

    def get(self, oid):
        self.flush()
        return self.proxy.get(self.name, oid)

    def findOne(self, query={}, select={}):
        self.flush()
        return self.proxy.findOne(self.name, json.dumps(query), json.dumps(select))

    def find(self, query={}, select={}):
        self.flush()
        label = self.proxy.find(self.name, json.dumps(query), json.dumps(select))
        return Cursor(label, self.proxy)

    def remove(self, query={}):
        self.flush()
        self.proxy.remove(self.name, json.dumps(query))
                      
    def flush(self):
        if len(self.batch) > 0:
            self._insertBatch()
            
    def ensureIndex(self, path):
        self.proxy.ensureIndex(self.name, path)
            


class Cursor(object):

    def __init__(self, label, conn):
        self.label = label
        self.conn = conn
        self.batch = []
        self.done = False
    
    def __iter__(self):
        return self
        
    def next(self):
        
        if self.done:
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

c = Connection('localhost:6060')
coll = c.getCollection('hyperbins_vert')

import pymongo
rem = pymongo.Connection('localhost:27018').patent_grants.hyperbins_vert

