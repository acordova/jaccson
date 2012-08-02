
from bson.son import SON
from cursor import Cursor

class Collection(object):

    def __init__(self, database, name, create=False, **kwargs):
        
        self._database = database
        self._name = name
        
        self.proxy = self._database.proxy
        self.batch = []
        self.batchSize = 100
    
    def __repr__(self):
         return "Collection(%r, %r)" % (self._database, self._name)

     def __eq__(self, other):
         if isinstance(other, Collection):
             us = (self._database, self._name)
             them = (other._database, other._name)
             return us == them
         return NotImplemented

     @property
     def full_name(self):
         """The full name of this :class:`Collection`.

         The full name is of the form `database_name.collection_name`.

         .. versionchanged:: 1.3
            ``full_name`` is now a property rather than a method.
         """
         return self._database.name + '.' + self._name
         
    @property
    def name(self):
        """The name of this :class:`Collection`.

        .. versionchanged:: 1.3
           ``name`` is now a property rather than a method.
        """
        return self._name

    @property
    def database(self):
       """The :class:`~pymongo.database.Database` that this
       :class:`Collection` is a part of.

       .. versionchanged:: 1.3
          ``database`` is now a property rather than a method.
       """
       return self._database
    
    def _insertBatch(self):
        self.proxy.insertBatch(self.name, self.batch)
        self.proxy.flush(self.name)
        self.batch = []
    
    def save(self, to_save, manipulate=True, safe=False, **kwargs):
        self.insert(to_save)
        
    def insert(self, doc_or_docs):

        docs = doc_or_docs
        if isinstance(docs, dict):
            docs = [docs]
        
        ids = []
        for doc in docs:
            if not jo.has_key('_id'):
                jo['_id'] = str(uuid.uuid4())
            
            _id = jo['_id']
        
            jos = json.dumps(jo)
        
            self.batch.append(jos)
            if len(self.batch) > self.batchSize:
                self._insertBatch()
            
            ids.append(_id)

        return len(ids) == 1 and ids[0] or ids

    def get(self, oid):
        self.flush()
        obj = self.proxy.get(self.name, oid)
        if obj is None:
            return None
        return json.loads(obj)

    def find_one(self, query={}, select={}):
        self.flush()
        obj = self.proxy.findOne(self.name, json.dumps(query), json.dumps(select))
        if obj is None:
            return None
        return json.loads(obj)

    def find(self, query={}, select={}):
        self.flush()
        label = self.proxy.find(self.name, json.dumps(query), json.dumps(select))
        return Cursor(label, self.proxy)

    def update(self, query={}, mods={}):
        self.flush()
        
        # batch?
        self.proxy.update(self.name, json.dumps(query), json.dumps(mods))
        
    def remove(self, query={}):
        self.flush()
        
        # batch?
        self.proxy.remove(self.name, json.dumps(query))
                      
    def flush(self):
        if len(self.batch) > 0:
            self._insertBatch()
            
    def ensureIndex(self, obj, block=False):
        self.proxy.ensureIndex(self.name, json.dumps(obj), block)
        
    def dropIndex(self, obj):
        self.proxy.dropIndex(self.name, json.dumps(obj));
        
    def drop(self):
        self.proxy.drop(self.name)
