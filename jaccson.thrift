namespace java com.jaccson.proxy

exception JaccsonException {
	1:string reason
}

service TableCursorService {

	void insertBatch(1:string db, 2:string coll, 3:list<string> json) throws (1:JaccsonException e),
	
	void update(1:string db, 2:string coll, 3:string query, 4:string mods) throws (1:JaccsonException e),
	
	i32 find(1:string db, 2:string coll, 3:string query, 4:string select) throws (1:JaccsonException e),
	
	string findOne(1:string db, 2:string coll, 3:string query, 4:string select) throws (1:JaccsonException e),
	
	string get(1:string db, 2:string coll, 3:string oid) throws (1:JaccsonException e),
	
	void remove(1:string db, 2:string coll, 3:string query) throws (1:JaccsonException e),
	
	void flush(1:string db, 2:string coll) throws (1:JaccsonException e),
	
	void ensureIndex(1:string db, 2:string coll, 3:string obj, 4:bool drop) throws (1:JaccsonException e),
	
	void dropIndex(1:string db, 2:string coll, 3:string obj) throws (1:JaccsonException e),
	
	void compact(1:string db, 2:string coll) throws (1:JaccsonException e),
	
	void dropCollection(1:string db, 2:string coll) throws (1:JaccsonException e),
	
	void dropDatabase(1:string db) throws (1:JaccsonException e),
	
	list<string> nextBatch(1:i32 cursor) throws (1:JaccsonException e)
	
}