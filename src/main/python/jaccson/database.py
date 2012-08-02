
from collection import Collection


class Database(object):
    """docstring for Database"""

    def __init__(self, connection, name):
        
        self._connection = connection
        self._name = name
    
    @property
    def connection(self):
        return self._connection
    
    @property
    def name(self):
        return self._name
    
    def __eq__(self, other):
        if isinstance(other, Database):
            us = (self.__connection, self.__name)
            them = (other.__connection, other.__name)
            return us == them
        return NotImplemented

    def __repr__(self):
        return "Database(%r, %r)" % (self.__connection, self.__name)

    def __getattr__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def __getitem__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return self.__getattr__(name)

    def create_collection(self, name, **kwargs):
        """Create a new :class:`~pymongo.collection.Collection` in this
        database.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.

        Options should be passed as keyword arguments to this
        method. Any of the following options are valid:

          - "size": desired initial size for the collection (in
            bytes). must be less than or equal to 10000000000. For
            capped collections this size is the max size of the
            collection.
          - "capped": if True, this is a capped collection
          - "max": maximum number of objects if capped (optional)

        :Parameters:
          - `name`: the name of the collection to create
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 2.2
           Removed deprecated argument: options

        .. versionchanged:: 1.5
           deprecating `options` in favor of kwargs
        """
        opts = {"create": True}
        opts.update(kwargs)

        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)

        return Collection(self, name, **opts)
    
    def collection_names(self):
        """Get a list of all the collection names in this database.
        """
        results = self["system.namespaces"].find(_must_use_master=True)
        names = [r["name"] for r in results]
        names = [n[len(self.__name) + 1:] for n in names
                 if n.startswith(self.__name + ".")]
        names = [n for n in names if "$" not in n]
        return names

    def drop_collection(self, name_or_collection):
        """Drop a collection.

        :Parameters:
          - `name_or_collection`: the name of a collection to drop or the
            collection object itself
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, basestring):
            raise TypeError("name_or_collection must be an instance of "
                            "%s or Collection" % (basestring.__name__,))

        self.__connection._purge_index(self.__name, name)

        self.command("drop", unicode(name), allowable_errors=["ns not found"])
    
    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Database' object is not iterable")

    def add_user(self, name, password, read_only=False):
        """Create user `name` with password `password`.

        Add a new user with permissions for this :class:`Database`.

        .. note:: Will change the password if user `name` already exists.

        :Parameters:
          - `name`: the name of the user to create
          - `password`: the password of the user to create
          - `read_only` (optional): if ``True`` it will make user read only

        .. versionchanged:: 2.2
           Added support for read only users

        .. versionadded:: 1.4
        """
        pwd = helpers._password_digest(name, password)
        self.system.users.update({"user": name},
                                 {"user": name,
                                  "pwd": pwd,
                                  "readOnly": read_only},
                                 upsert=True, safe=True)

    def remove_user(self, name):
        """Remove user `name` from this :class:`Database`.

        User `name` will no longer have permissions to access this
        :class:`Database`.

        :Parameters:
          - `name`: the name of the user to remove

        .. versionadded:: 1.4
        """
        self.system.users.remove({"user": name}, safe=True)

    def authenticate(self, name, password):
        """Authenticate to use this database.

        Once authenticated, the user has full read and write access to
        this database. Raises :class:`TypeError` if either `name` or
        `password` is not an instance of :class:`basestring`
        (:class:`str` in python 3). Authentication lasts for the life
        of the underlying :class:`~pymongo.connection.Connection`, or
        until :meth:`logout` is called.

        The "admin" database is special. Authenticating on "admin"
        gives access to *all* databases. Effectively, "admin" access
        means root access to the database.

        .. note::
          This method authenticates the current connection, and
          will also cause all new :class:`~socket.socket` connections
          in the underlying :class:`~pymongo.connection.Connection` to
          be authenticated automatically.

         - When sharing a :class:`~pymongo.connection.Connection`
           between multiple threads, all threads will share the
           authentication. If you need different authentication profiles
           for different purposes (e.g. admin users) you must use
           distinct instances of :class:`~pymongo.connection.Connection`.

         - To get authentication to apply immediately to all
           existing sockets you may need to reset this Connection's
           sockets using :meth:`~pymongo.connection.Connection.disconnect`.

        .. warning::

          Currently, calls to
          :meth:`~pymongo.connection.Connection.end_request` will
          lead to unpredictable behavior in combination with
          auth. The :class:`~socket.socket` owned by the calling
          thread will be returned to the pool, so whichever thread
          uses that :class:`~socket.socket` next will have whatever
          permissions were granted to the calling thread.

        :Parameters:
          - `name`: the name of the user to authenticate
          - `password`: the password of the user to authenticate

        .. mongodoc:: authenticate
        """
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance "
                            "of %s" % (basestring.__name__,))
        if not isinstance(password, basestring):
            raise TypeError("password must be an instance "
                            "of %s" % (basestring.__name__,))

        in_request = self.connection.in_request()
        try:
            if not in_request:
                self.connection.start_request()

            nonce = self.command("getnonce")["nonce"]
            key = helpers._auth_key(nonce, name, password)
            try:
                self.command("authenticate", user=unicode(name),
                             nonce=nonce, key=key)
                self.connection._cache_credentials(self.name,
                                                   unicode(name),
                                                   unicode(password))
                return True
            except OperationFailure:
                return False
        finally:
            if not in_request:
                self.connection.end_request()

    def logout(self):
        """Deauthorize use of this database for this connection
        and future connections.

        .. note:: Other databases may still be authenticated, and other
           existing :class:`~socket.socket` connections may remain
           authenticated for this database unless you reset all sockets
           with :meth:`~pymongo.connection.Connection.disconnect`.
        """
        self.command("logout")
        self.connection._purge_credentials(self.name)

        
