package com.jaccson;

import java.util.Iterator;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.JSONException;
import org.json.JSONObject;


public class JaccsonCursor implements Iterable<JSONObject>, Iterator<JSONObject> {
	
	private Iterator<JSONObject> mainIterator;
	
	public JaccsonCursor(JaccsonTable table, JSONObject query, JSONObject select) throws TableNotFoundException, JSONException {

		// if query is {}, then create basic scanner
		if(query == null || query.length() == 0) {
			Scanner scanner = table.conn.createScanner(table.tableName, table.auths);
			mainIterator = new UnfilteredJSONIterator(scanner.iterator());
		}
		else {
			// JSON query is a simple set of expressions ANDed
			
			IndexScanner ijs = new IndexScanner(query, table);
			
			JSONObject uic = ijs.getUnindexedClauses();
			if(uic == null) { // all clauses use an index
				mainIterator = new UnfilteredJSONIterator(ijs);
			}
			else {
				if(!ijs.isUsingIndexes()) { // have query clauses and no indexes
					Scanner scanner = table.conn.createScanner(table.tableName, table.auths);
					mainIterator = new FilteredJSONIterator(scanner.iterator(), uic);
				}
				else { // have some indexes and some unindexed clauses
					mainIterator = new FilteredJSONIterator(ijs, uic);
				}
			}
		}
		
		// TODO: set select clause as scan filter
	}

	public Iterator<JSONObject> iterator() {
		
		return mainIterator;
	}

	public long count() {
		// TODO: implement
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

