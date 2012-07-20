package com.jaccson;

import java.util.Iterator;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.server.SelectIterator;
import com.jaccson.server.TableScanIterator;


public class JaccsonCursor implements Iterable<JSONObject>, Iterator<JSONObject> {
	
	private Iterator<JSONObject> mainIterator;
	
	public JaccsonCursor(JaccsonTable table, JSONObject query, JSONObject select) throws TableNotFoundException, JSONException {

		// if query is {}, then create basic scanner
		if(query == null || query.length() == 0) {
			
			Scanner scanner = table.conn.createScanner(table.tableName, table.auths);
			if(select != null && !select.toString().equals("{}")) {
				SelectIterator.setSelectOnScanner(scanner, select);
			}
			
			mainIterator = new JSONIterator(scanner.iterator());			
		}
		else {
			// JSON query is a simple set of expressions ANDed
			
			IndexScanner ijs = new IndexScanner(query, table);
			
			if(select != null && !select.toString().equals(""))
				ijs.setFilter(select);
			
			mainIterator = new JSONIterator(ijs);
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

