package com.jaccson.mongo;

import java.util.Iterator;
import java.util.List;

import com.mongodb.DBCollection;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public interface CurInterface {

	public abstract com.mongodb.DBCursor copy();

	public abstract Iterator<DBObject> iterator();

	public abstract com.mongodb.DBCursor sort(DBObject orderBy);

	public abstract com.mongodb.DBCursor addSpecial(String name, Object o);

	public abstract com.mongodb.DBCursor hint(String indexName);

	public abstract com.mongodb.DBCursor snapshot();

	public abstract com.mongodb.DBCursor limit(int n);

	public abstract com.mongodb.DBCursor batchSize(int n);

	public abstract com.mongodb.DBCursor skip(int n);

	public abstract long getCursorId();

	public abstract void close();

	public abstract com.mongodb.DBCursor slaveOk();

	public abstract com.mongodb.DBCursor addOption(int option);

	public abstract com.mongodb.DBCursor setOptions(int options);

	public abstract com.mongodb.DBCursor resetOptions();

	public abstract int getOptions();

	public abstract int numGetMores();

	public abstract List<Integer> getSizes();

	public abstract int numSeen();

	public abstract boolean hasNext() throws MongoException;

	public abstract DBObject next() throws MongoException;

	public abstract DBObject curr();

	public abstract void remove();

	public abstract int length() throws MongoException;

	public abstract List<DBObject> toArray() throws MongoException;

	public abstract List<DBObject> toArray(int max) throws MongoException;

	public abstract int itcount();

	public abstract int count() throws MongoException;

	public abstract boolean equals(Object obj);

	public abstract DBObject explain();

	public abstract int size() throws MongoException;

	public abstract DBObject getKeysWanted();

	public abstract DBObject getQuery();

	public abstract DBCollection getCollection();

	public abstract ServerAddress getServerAddress();

	public abstract com.mongodb.DBCursor setReadPreference(
			ReadPreference preference);

	public abstract ReadPreference getReadPreference();

	public abstract com.mongodb.DBCursor setDecoderFactory(DBDecoderFactory fact);

	public abstract DBDecoderFactory getDecoderFactory();

	public abstract int hashCode();

	public abstract com.mongodb.DBCursor hint(DBObject indexKeys);

	public abstract String toString();

}