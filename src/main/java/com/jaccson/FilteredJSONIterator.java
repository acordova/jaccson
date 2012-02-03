/**
 * This class converts KeyValue pairs into JSON objects and filters objects
 * based on the query clauses that don't have indexes
 * 
 */

package com.jaccson;


import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class FilteredJSONIterator implements Iterator<JSONObject>, Iterable<JSONObject>{

	private JSONObject filter;
	private Iterator<Entry<Key, Value>> iter;
	private JSONObject nextMatch = null;
	private boolean spent = false;
	
	public FilteredJSONIterator(Iterator<Entry<Key,Value>> iter, JSONObject filter) {
		this.filter = filter;
		this.iter = iter;
	}

	@Override
	public Iterator<JSONObject> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {

		if(spent) return false;
		
		if(nextMatch == null)
			advanceIter();
			
		return !spent;
	}

	@Override
	public JSONObject next() {
		JSONObject o = nextMatch;
		advanceIter();
		return o;
	}

	@Override
	public void remove() {}

	private void advanceIter() {
		if(!iter.hasNext()) {
			spent = true;
			return;
		}
		
		// scan for next matching object
		// this is the part some people would want to do at the tablet server
		nextMatch = JSONHelper.objectForKeyValue(iter.next());
		while(!satisfiesFilter(nextMatch)) {
			if(!iter.hasNext()) {
				spent = true;
				return;
			}
			nextMatch = JSONHelper.objectForKeyValue(iter.next());
		}
	}
	
	private boolean satisfiesFilter(JSONObject o) {
		
		for(String n : JSONObject.getNames(filter)) {
			try {
				// make this an array, one of which can satisfy the filter?
				Object value = JSONHelper.valueForPath(n, o);
				
				if(value == null)
					return false;
				
				if(!satisfiesClause(value, n, filter.get(n)))
					return false;
				
			} catch (JSONException e) { // field not found
				//e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}

	private static boolean satisfiesClause(Object value, String field, Object clause) throws JSONException {

		if(clause instanceof JSONObject) { // some op other than eq

			JSONObject obj = (JSONObject)clause;

			// get inner value
			String op = JSONObject.getNames(obj)[0];
			Object ival = obj.get(field);

			// TODO: add gte and lte
			if(op.equals("$gt")) {
				if(ival instanceof Integer) {
					return (Integer)value > (Integer)clause;
				}
				else if(ival instanceof Double) {
					return (Double)value > (Double)clause;
				}
				else if(ival instanceof String) {
					return ((String)ival).compareTo((String)clause) > 0;
				}

			} else if(op.equals("$lt")) {
				if(ival instanceof Integer) {
					return (Integer)value < (Integer)clause;
				}
				else if(ival instanceof Double) {
					return (Double)value < (Double)clause;
				}
				else if(ival instanceof String) {
					return ((String)value).compareTo((String)clause) < 0;
				}

			} else if(op.equals("$between")) {
				// expect a JSON Array next
				JSONArray ar = (JSONArray)ival;

				Object first = ar.get(0);
				Object last = ar.get(1);
				
				if(value instanceof Integer && first instanceof Integer && last instanceof Integer) {
					if((Integer)first > (Integer)last) {
						Object tmp = last;
						last = first;
						first = tmp;
					}
					
					return((Integer)value > (Integer)first && (Integer)value < (Integer)last);
				}
				
				else if(value instanceof Double && first instanceof Double && last instanceof Double) {
					if((Double)first > (Double)last) {
						Object tmp = last;
						last = first;
						first = tmp;
					}
					
					return((Double)value > (Double)first && (Double)value < (Double)last);
				}
				
				else if(value instanceof String && first instanceof String && last instanceof String) {
					if(((String)first).compareTo((String)last) > 0) {
						Object tmp = last;
						last = first;
						first = tmp;
					}
					
					return(((String)value).compareTo((String)first) > 0 
							&& ((String)value).compareTo((String)last) < 0);
 
				}

			} else if(op.equals("$in")) {
				JSONArray ar = (JSONArray)ival;

				for(int i=0; i < ar.length(); i++) {
					Object ivalo = ar.get(i);
					
					if(ival instanceof Integer && ivalo instanceof Integer) {
						if(ival == ivalo)
							return true;
					}
					else if(ival instanceof Double && ivalo instanceof Double) {
						if(ival == ivalo)
							return true;
					}
					else if(ival instanceof String && ivalo instanceof String) {
						if(((String)ival).equals((String)clause))
							return true;
					}
				}
			}

			// equality
		} 
		else if(clause instanceof Integer || clause instanceof Double) {
			return value == clause;
		}
		else if(clause instanceof String) {
			return value.equals(clause);
		}

		return false;
	}
}
