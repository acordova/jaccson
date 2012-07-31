package com.jaccson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.jaccson.server.QueryFilter;
import com.jaccson.server.SelectIterator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class DBCursor implements Iterator<DBObject>, Iterable<DBObject> {


	private DBCollection coll;
	private DBObjectIterator mainIterator;
	private long read;
	private long limit = -1;
	private DBObject query;


	DBCursor(DBCollection dbCollection, DBObject query, DBObject select, int numToSkip) throws TableNotFoundException {
		
		coll = dbCollection;
		this.query = query;
		
		// no query
		if(query == null || query.keySet().size() == 0 || query.toString().equals("{}")) {

			Scanner scanner = coll.conn.createScanner(coll.tableName, coll.auths);
			// have select
			if(select != null && !select.toString().equals("{}")) {
				SelectIterator.setSelectOnScanner(scanner, select);
			}

			mainIterator = new DBObjectIterator(scanner.iterator());			
		}
		// have query
		else {

			// figure out which clauses can use an index			
			HashMap<String, Object> indexedClauses = new HashMap<String, Object>();
			DBObject unindexedClauses = new BasicDBObject();

			for(String name : query.keySet()) {
				if(!coll.isIndexed(name)) {
					unindexedClauses.put(name, query.get(name));
				}
				else {
					indexedClauses.put(name, query.get(name));
				}
			}

			if(indexedClauses.size() == 0) {
				Scanner scanner = coll.conn.createScanner(coll.tableName, coll.auths);

				// all clauses are unindexed, push original query to the servers
				QueryFilter.setFilterOnScanner(scanner, query);

				// have select
				if(select != null && !select.toString().equals("{}")) {
					SelectIterator.setSelectOnScanner(scanner, select);
				}

				mainIterator = new DBObjectIterator(scanner.iterator());	
			}
			else {
				IndexScanner ijs = new IndexScanner(indexedClauses, unindexedClauses, select, coll);

				mainIterator = new DBObjectIterator(ijs);
			}
		}
	}



	public Iterator<DBObject> iterator() {

		return mainIterator;
	}


	@Override
	public boolean hasNext() {
		return read < limit && mainIterator.hasNext();
	}

	@Override
	public DBObject next() {
		read++;
		return mainIterator.next();
	}

	@Override
	public void remove() {}


	public DBCursor copy() {
		// TODO Auto-generated method stub
		return null;
	}




	public DBCursor sort(DBObject orderBy) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor addSpecial(String name, Object o) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor hint(String indexName) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor snapshot() {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor limit(int n) {
		limit = n;
		return this;
	}


	public DBCursor batchSize(int n) {
		// TODO Auto-generated method stub
		return this;
	}


	public DBCursor skip(int n) {
		for(int i=0; i < n && hasNext(); i++)
			next();
		
		return this;
	}


	public long getCursorId() {

		return 0;
	}


	public void close() {
		// TODO Auto-generated method stub
	}


	public DBCursor slaveOk() {
		return this;
	}


	public DBCursor addOption(int option) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor setOptions(int options) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor resetOptions() {
		// TODO Auto-generated method stub
		return null;
	}


	public int getOptions() {
		// TODO Auto-generated method stub
		return 0;
	}


	public int numGetMores() {
		// TODO Auto-generated method stub
		return 0;
	}


	public List<Integer> getSizes() {
		// TODO Auto-generated method stub
		return null;
	}


	public int numSeen() {
		return (int)read;
	}



	public DBObject curr() {
		// TODO Auto-generated method stub
		return null;
	}


	public int length() throws MongoException {
		return 0;
	}


	/**
	 * machine will probably run out of memory before we hit max
	 * 
	 * @return
	 * @throws MongoException
	 */
	public List<DBObject> toArray() throws MongoException {
		return toArray(Integer.MAX_VALUE);
	}

	public List<DBObject> toArray(int max) throws MongoException {
		ArrayList<DBObject> list = new ArrayList<DBObject>();
		
		for(int i=0; i < max && hasNext(); i++)
			list.add(next());
		
		return list;
	}


	public int itcount() {
		// TODO Auto-generated method stub
		return 0;
	}


	public int count() throws MongoException {
		int c;
		for(c = 0; hasNext(); c++)
			next();
		
		return c;
	}


	public DBObject explain() {
		// TODO Auto-generated method stub
		return null;
	}


	public int size() throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public DBObject getKeysWanted() {
		// TODO Auto-generated method stub
		return null;
	}


	public DBObject getQuery() {
		return query;
	}


	public DBCollection getCollection() {
		return coll;
	}


	public ServerAddress getServerAddress() {
		return null;
	}


	public DBCursor setReadPreference(ReadPreference preference) {
		// TODO Auto-generated method stub
		return null;
	}


	public ReadPreference getReadPreference() {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor setDecoderFactory(DBDecoderFactory fact) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBDecoderFactory getDecoderFactory() {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCursor hint(DBObject indexKeys) {
		// TODO Auto-generated method stub
		return null;
	}

}
