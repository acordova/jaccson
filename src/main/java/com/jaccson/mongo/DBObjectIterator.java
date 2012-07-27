package com.jaccson.mongo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.mongodb.DBObject;

public class DBObjectIterator implements Iterator<DBObject> {
	
	private Iterator<Entry<Key, Value>> iter;

	public DBObjectIterator(Iterator<Entry<Key,Value>> iter) {
		this.iter = iter; 
	}

	@Override
	public boolean hasNext() {

		return iter.hasNext();
	}

	@Override
	public DBObject next() {
		
		return BSONHelper.objectForEntry(iter.next());
	}

	@Override
	public void remove() { }
}
