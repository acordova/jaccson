package com.jaccson.mongo;

import java.util.Iterator;
import java.util.List;

import com.mongodb.DBDecoderFactory;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class DBCursor {

	
	public com.mongodb.DBCursor copy() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public Iterator<DBObject> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor sort(DBObject orderBy) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor addSpecial(String name, Object o) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor hint(String indexName) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor snapshot() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor limit(int n) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor batchSize(int n) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor skip(int n) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public long getCursorId() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
	public com.mongodb.DBCursor slaveOk() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor addOption(int option) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor setOptions(int options) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor resetOptions() {
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
		// TODO Auto-generated method stub
		return 0;
	}

	
	public boolean hasNext() throws MongoException {
		// TODO Auto-generated method stub
		return false;
	}

	
	public DBObject next() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject curr() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void remove() {
		// TODO Auto-generated method stub
		
	}

	
	public int length() throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public List<DBObject> toArray() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public List<DBObject> toArray(int max) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public int itcount() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public int count() throws MongoException {
		// TODO Auto-generated method stub
		return 0;
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
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBCollection getCollection() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public ServerAddress getServerAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor setReadPreference(ReadPreference preference) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public ReadPreference getReadPreference() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor setDecoderFactory(DBDecoderFactory fact) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBDecoderFactory getDecoderFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCursor hint(DBObject indexKeys) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
