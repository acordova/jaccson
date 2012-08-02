
import sys
sys.path.append('gen-py')

from jaccson import TableCursorService
from jaccson.ttypes import *

from thrift import Thrift, TApplicationException
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from connection import Connection


#c = Connection('localhost:6060')
#coll = c.getCollection('hyperbins_vert')

#import pymongo
#rem = pymongo.Connection('localhost:27018').patent_grants.hyperbins_vert

