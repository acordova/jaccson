package com.jaccson;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.server.QueryFilter;
import com.jaccson.server.SelectIterator;


public class JaccsonCursor implements Iterable<JSONObject>, Iterator<JSONObject> {
	
	private Iterator<JSONObject> mainIterator;
	
	public JaccsonCursor(JaccsonTable table, JSONObject query, JSONObject select) throws TableNotFoundException, JSONException {

		// no query
		if(query == null || query.length() == 0 || query.toString().equals("{}")) {
			
			Scanner scanner = table.conn.createScanner(table.tableName, table.auths);
			// have select
			if(select != null && !select.toString().equals("{}")) {
				SelectIterator.setSelectOnScanner(scanner, select);
			}
			
			mainIterator = new JSONIterator(scanner.iterator());			
		}
		// have query
		else {
			
			// figure out which clauses can use an index			
			HashMap<String, Object> indexedClauses = new HashMap<String, Object>();
			JSONObject unindexedClauses = new JSONObject();
			
			for(String name : JSONObject.getNames(query)) {
				if(!table.isIndexed(name)) {
					unindexedClauses.put(name, query.get(name));
				}
				else {
					indexedClauses.put(name, query.get(name));
				}
			}
			
			if(indexedClauses.size() == 0) {
				Scanner scanner = table.conn.createScanner(table.tableName, table.auths);
				
				// all clauses are unindexed, push original query to the servers
				QueryFilter.setFilterOnScanner(scanner, query);
				
				// have select
				if(select != null && !select.toString().equals("{}")) {
					SelectIterator.setSelectOnScanner(scanner, select);
				}
				
				mainIterator = new JSONIterator(scanner.iterator());	
			}
			else {
				IndexScanner ijs = new IndexScanner(indexedClauses, unindexedClauses, select, table);
				
				mainIterator = new JSONIterator(ijs);
			}
		}
	}
	
	

	public Iterator<JSONObject> iterator() {
		
		return mainIterator;
	}

	public long count() {
		// TODO: implement
		// could try to estimate somehow
		return 0L;
	}

	@Override
	public boolean hasNext() {
		return mainIterator.hasNext();
	}

	@Override
	public JSONObject next() {
		return mainIterator.next();
	}

	@Override
	public void remove() {}
}

