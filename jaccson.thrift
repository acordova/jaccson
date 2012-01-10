namespace java com.jaccson.proxy

exception JaccsonException {
	1:string reason
}

service TableCursorService {

	void insert(1:string table, 2:string json),
	
	void update(1:string table, 2:string query, 3:string mods),
	
	i32 find(1:string table, 2:string query, 3:string select),
	
	string findOne(1:string table, 2:string query, 3:string select),
	
	string get(1:string table, 2:string oid),
	
	void remove(1:string table, 2:string query),
	
	void flush(1:string table),
	
	void ensureIndex(1:string table, 2:string path),
	
	void dropIndex(1:string table, 2:string path),
	
	void compact(1:string table),
	
	void drop(1:string table),
	
	list<string> nextBatch(1:i32 cursor)
	
}