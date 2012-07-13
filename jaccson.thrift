namespace java com.jaccson.proxy

exception JaccsonException {
	1:string reason
}

service TableCursorService {

	void insertBatch(1:string table, 2:list<string> json) throws (1:JaccsonException e),
	
	void update(1:string table, 2:string query, 3:string mods) throws (1:JaccsonException e),
	
	i32 find(1:string table, 2:string query, 3:string select) throws (1:JaccsonException e),
	
	string findOne(1:string table, 2:string query, 3:string select) throws (1:JaccsonException e),
	
	string get(1:string table, 2:string oid) throws (1:JaccsonException e),
	
	void remove(1:string table, 2:string query) throws (1:JaccsonException e),
	
	void flush(1:string table) throws (1:JaccsonException e),
	
	void ensureIndex(1:string table, 2:string obj, 3:bool drop) throws (1:JaccsonException e),
	
	void dropIndex(1:string table, 2:string obj) throws (1:JaccsonException e),
	
	void compact(1:string table) throws (1:JaccsonException e),
	
	void drop(1:string table) throws (1:JaccsonException e),
	
	list<string> nextBatch(1:i32 cursor) throws (1:JaccsonException e)
	
}