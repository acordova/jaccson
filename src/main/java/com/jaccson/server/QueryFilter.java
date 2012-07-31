package com.jaccson.server;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.jaccson.BSONHelper;
import com.jaccson.IterStack;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * This class is used to filter documents based on a clause when no indexes are available,
 * i.e. when doing a table scan
 * 
 * @author aaron
 *
 */
public class QueryFilter extends Filter {

	DBObject filter;

	@Override
	public boolean accept(Key k, Value v) {

		DBObject o = BSONHelper.objectForKeyValue(k, v);
		return satisfiesFilter(o, filter);
	}

	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		
		filter = (DBObject) JSON.parse(options.get("filter"));
	}

	@Override
	public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
		return null;
	}

	@Override
	public IteratorOptions describeOptions() {
		return null;
	}

	@Override
	public boolean validateOptions(Map<String,String> options) {
		return false;
	}


	public static void setFilterOnScanner(Scanner scanner, DBObject filter) {
		IteratorSetting filterIterSetting = new IteratorSetting(IterStack.FILTER_ITERATOR_PRI, "jaccsonFilter", "com.jaccson.server.QueryFilter");
		QueryFilter.setFilter(filterIterSetting, filter.toString());
		scanner.addScanIterator(filterIterSetting);
	}

	private static void setFilter(IteratorSetting filterIterSetting, String filter) {
		filterIterSetting.addOption("filter", filter);
	}

	public static void setFilterOnScanner(BatchScanner bscan, DBObject filter) {
		IteratorSetting filterIterSetting = new IteratorSetting(IterStack.FILTER_ITERATOR_PRI, "jaccsonFilter", "com.jaccson.server.QueryFilter");
		QueryFilter.setFilter(filterIterSetting, filter.toString());
		bscan.addScanIterator(filterIterSetting);
	}

	public static boolean satisfiesFilter(DBObject o, DBObject filter) {

		for(String n : filter.keySet()) {
			// make this an array, one of which can satisfy the filter?
			Object value = BSONHelper.valueForPath(n, o);

			if(value == null)
				return false;

			if(!satisfiesClause(value, n, filter.get(n)))
				return false;
		}

		return true;
	}

	private static boolean satisfiesClause(Object value, String field, Object clause) {

		if(clause == null)
			return false;
		
		if(clause instanceof DBObject) { // some op other than eq

			DBObject obj = (DBObject)clause;

			// get inner value
			String op = obj.keySet().iterator().next();
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
				BasicDBList ar = (BasicDBList)ival;

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
				BasicDBList ar = (BasicDBList)ival;

				for(int i=0; i < ar.size(); i++) {
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
