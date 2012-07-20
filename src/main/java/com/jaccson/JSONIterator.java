/**
 * This is a simple class that converts KeyValue pairs into JSONObjects
 * This is used for index scanners or full table scans with no filtering
 * 
 */
package com.jaccson;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.JSONObject;

public class JSONIterator implements Iterator<JSONObject> {

	private Iterator<Entry<Key, Value>> iter;

	public JSONIterator(Iterator<Entry<Key,Value>> iter) {
		this.iter = iter; 
	}

	@Override
	public boolean hasNext() {

		return iter.hasNext();
	}

	@Override
	public JSONObject next() {
		
		return JSONHelper.objectForEntry(iter.next());
	}

	@Override
	public void remove() { }
}
