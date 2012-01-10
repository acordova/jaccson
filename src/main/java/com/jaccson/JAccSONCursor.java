package com.jaccson;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.JSONException;
import org.json.JSONObject;


public class JAccSONCursor implements Iterable<JSONObject>, Iterator<JSONObject> {
	
	private Iterator<Entry<Key, Value>> mainIterator;
	
	public JAccSONCursor(JAccSONTable table, String query, String select) throws TableNotFoundException, JSONException {

		// if query is {}, then create basic scanner
		if(query == null || query.equals("{}")) {
			Scanner scanner = table.conn.createScanner(table.tableName, table.auths);
			mainIterator = scanner.iterator();
		}
		else {
			// JSON query is a simple set of expressions ANDed
			JSONObject q = new JSONObject(query);
			
			IndexJSONScanner ijs = new IndexJSONScanner(q, table);
			mainIterator = ijs;
		}
		
		// TODO: set select clause as scan filter
	}

	public Iterator<JSONObject> iterator() {
		return this;
	}

	public boolean hasNext() {
			
		return mainIterator.hasNext();
	}

	public JSONObject next() {
		
		Entry<Key,Value> e = mainIterator.next();
		
		// deserialize
		JSONObject o = null;
		try {
			o = new JSONObject(new String(e.getValue().get()));
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// add key 
		try {
			o.put("_id", e.getKey().getRow().toString());
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return o;
	}

	public void remove() {}
	
	public long count() {
		// TODO: implement
		return 0L;
	}
}

