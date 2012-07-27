package com.jaccson.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.jaccson.server.QueryFilter;
import com.jaccson.server.SelectIterator;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * this class designed to grab a batches from a set of indexes
 * 
 * @author aaron
 *
 */
public class IndexScanner implements Iterator<Entry<Key,Value>> {

	private BatchScanner bscan;
	private DBCollection coll;
	private Iterator<List<Range>> indexesIter; 
	private Iterator<Entry<Key,Value>> currentIter = null; 
	
	public IndexScanner(HashMap<String, Object> indexedClauses, DBObject unindexedClauses, DBObject select, DBCollection coll) throws TableNotFoundException {
		
		this.coll = coll;
		this.bscan = coll.batchScanner();
		
		// get unindexedClauses
		ArrayList<Iterator<Entry<Key,Value>>> iters = new ArrayList<Iterator<Entry<Key,Value>>>();
		
		for(Entry<String,Object> clause : indexedClauses.entrySet()) {
			Iterator<Entry<Key,Value>> i = iterForExpression(clause.getKey(), clause.getValue());
			iters.add(i);
		}
		
		if(iters.size() == 1) {
			indexesIter = new RowIDBatchIter(iters.get(0));
		}
		else { 
			indexesIter = new AndIters(iters);
		}
		
		if(unindexedClauses != null && unindexedClauses.keySet().size() > 0) {
			QueryFilter.setFilterOnScanner(bscan, unindexedClauses);
		}
		
		if(select != null && select.keySet().size() > 0) {
			SelectIterator.setSelectOnScanner(bscan, select);
		}
	}
	
	
	public boolean hasNext() {
		
		if(currentIter == null || !currentIter.hasNext()) {
			if(indexesIter.hasNext()) {
				List<Range> rows = indexesIter.next();
				if(rows.size() == 0)
					return false;
				
				bscan.setRanges(rows);
				currentIter = bscan.iterator();
				
			} else {
				return false;
			}
		}
		
		return true;
	}

	public Entry<Key, Value> next() {

		return currentIter.next();
	}

	public void remove() {}

	private Iterator<Entry<Key,Value>> iterForExpression(String field, Object value) throws TableNotFoundException {
		
		if(value instanceof DBObject) { // some op other than eq
			
			DBObject obj = (DBObject)value;
			
			// get inner value
			String op = obj.keySet().iterator().next();
			Object ival = obj.get(op);
			
			// TODO: add gte and lte
			if(op.equals("$gt")) {
				byte[] bytes = IndexHelper.indexValueForObject(ival);
				Scanner iscan = coll.indexScannerForKey(field);
				iscan.setRange(new Range(new Text(bytes), false, null, true));
				
				return iscan.iterator();
				
			} else if(op.equals("$lt")) {
				byte[] bytes = IndexHelper.indexValueForObject(ival);

				Scanner iscan = coll.indexScannerForKey(field);
				iscan.setRange(new Range(null, true, new Text(bytes), false));
				
				return iscan.iterator();
				
			} else if(op.equals("$between")) {
				// expect a JSON Array next
				BasicDBList ar = (BasicDBList)ival;
				
				byte[] bytesStart = IndexHelper.indexValueForObject(ar.get(0));
				byte[] bytesEnd = IndexHelper.indexValueForObject(ar.get(1));
				
				Scanner iscan = coll.indexScannerForKey(field);
				iscan.setRange(new Range(new Text(bytesStart), true, new Text(bytesEnd), true));
				
				return iscan.iterator();
				
			} else if(op.equals("$in")) {
				BasicDBList ar = (BasicDBList)ival;
				ArrayList<Range> ranges = new ArrayList<Range>();
				
				for(int i=0; i < ar.size(); i++) {
					ranges.add(new Range(new Text(IndexHelper.indexValueForObject(ar.get(i)))));
				}
				
				BatchScanner biscan = coll.batchScannerForKey(field);
				biscan.setRanges(ranges);
				return biscan.iterator();
			}
			
		// equality
		} else if(value instanceof Integer || value instanceof Double || value instanceof String) {
			
			Scanner iscan = coll.indexScannerForKey(field);
			iscan.setRange(new Range(new Text(IndexHelper.indexValueForObject(value))));
			
			return iscan.iterator();
		}
		
		return null;
	}
}
