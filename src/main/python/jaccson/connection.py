
from jaccson import TableCursorService
from jaccson.ttypes import *

from thrift import Thrift, TApplicationException
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import database

class Connection(object):

    def __init__(self, host="localhost", port=21017, max_pool_size=10,
                 network_timeout=None, document_class=dict,
                 tz_aware=False, _connect=True, **kwargs):

        self._host = host
        self._port = port
        
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

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port
    
    def disconnect(self):
        self.transport.close()
        
    def close(self):
        # Close!
        self.disconnect()

    def getCollection(self, cname):
        return Collection(cname, self)
        
    def __eq__(self, other):
        if isinstance(other, Connection):
            us = (self._host, self._port)
            them = (other._host, other._port)
            return us == them
        return NotImplemented

    def __repr__(self):
        return "Connection(%r, %r)" % (self._host, self._port)

    def __getattr__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return database.Database(self, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def close_cursor(self, cursor_id):
        """Close a single database cursor.

        Raises :class:`TypeError` if `cursor_id` is not an instance of
        ``(int, long)``. What closing the cursor actually means
        depends on this connection's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close

        .. seealso:: :meth:`set_cursor_manager` and
           the :mod:`~pymongo.cursor_manager` module
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__cursor_manager.close(cursor_id)

    def kill_cursors(self, cursor_ids):
        """Send a kill cursors message with the given ids.

        Raises :class:`TypeError` if `cursor_ids` is not an instance of
        ``list``.

        :Parameters:
          - `cursor_ids`: list of cursor ids to kill
        """
        if not isinstance(cursor_ids, list):
            raise TypeError("cursor_ids must be a list")
        return self._send_message(message.kill_cursors(cursor_ids))

    def server_info(self):
        """Get information about the MongoDB server we're connected to.
        """
        return self.admin.command("buildinfo")

    def database_names(self):
        """Get a list of the names of all databases on the connected server.
        """

    def drop_database(self, name_or_database):
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`basestring` (:class:`str` in python 3) or Database.

        :Parameters:
          - `name_or_database`: the name of a database to drop, or a
            :class:`~pymongo.database.Database` instance representing the
            database to drop
        """
        name = name_or_database
        if isinstance(name, database.Database):
            name = name.name

        if not isinstance(name, basestring):
            raise TypeError("name_or_database must be an instance of "
                            "%s or Database" % (basestring.__name__,))


    def copy_database(self, from_name, to_name,
                      from_host=None, username=None, password=None):
        """Copy a database, potentially from another host.

        Raises :class:`TypeError` if `from_name` or `to_name` is not
        an instance of :class:`basestring` (:class:`str` in python 3).
        Raises :class:`~pymongo.errors.InvalidName` if `to_name` is
        not a valid database name.

        If `from_host` is ``None`` the current host is used as the
        source. Otherwise the database is copied from `from_host`.

        If the source database requires authentication, `username` and
        `password` must be specified.

        :Parameters:
          - `from_name`: the name of the source database
          - `to_name`: the name of the target database
          - `from_host` (optional): host name to copy from
          - `username` (optional): username for source database
          - `password` (optional): password for source database

        .. note:: Specifying `username` and `password` requires server
           version **>= 1.3.3+**.

        .. versionadded:: 1.5
        """
        pass

    @property
    def is_locked(self):
        """Is this server locked? While locked, all write operations
        are blocked, although read operations may still be allowed.
        Use :meth:`~pymongo.connection.Connection.unlock` to unlock.

        .. versionadded:: 2.0
        """
        return False

    def fsync(self, **kwargs):
        """Flush all pending writes to datafiles.

        :Parameters:

            Optional parameters can be passed as keyword arguments:

            - `lock`: If True lock the server to disallow writes.
            - `async`: If True don't block while synchronizing.

            .. warning:: `async` and `lock` can not be used together.

            .. warning:: MongoDB does not support the `async` option
                         on Windows and will raise an exception on that
                         platform.

        .. versionadded:: 2.0
        """
        pass

    def unlock(self):
        """Unlock a previously locked server.

        .. versionadded:: 2.0
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Connection' object is not iterable")
