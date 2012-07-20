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
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.FilteredJSONIterator;
import com.jaccson.IterStack;
import com.jaccson.JSONHelper;

/**
 * This class is used to filter documents based on a clause when no indexes are available,
 * i.e. when doing a table scan
 * 
 * @author aaron
 *
 */
public class TableScanIterator extends Filter {

	JSONObject filter;
	
	@Override
	  public boolean accept(Key k, Value v) {
		
		JSONObject o = JSONHelper.objectForKeyValue(k, v);
	    return FilteredJSONIterator.satisfiesFilter(o, filter);
	  }
	  
	  @Override
	  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		  super.init(source, options, env);
			try {
				filter = new JSONObject(options.get("filter"));
			} catch (JSONException e) {
				throw new IOException(e);
			}
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
	  
	
	public static void setFilterOnScanner(Scanner scanner, JSONObject filter) {
		IteratorSetting filterIterSetting = new IteratorSetting(IterStack.FILTER_ITERATOR_PRI, "jaccsonFilter", "com.jaccson.server.TableScanIterator");
		TableScanIterator.setFilter(filterIterSetting, filter.toString());
		scanner.addScanIterator(filterIterSetting);
	}

	private static void setFilter(IteratorSetting filterIterSetting, String filter) {
		filterIterSetting.addOption("filter", filter);
	}

	public static void setFilterOnScanner(BatchScanner bscan, JSONObject filter) {
		IteratorSetting filterIterSetting = new IteratorSetting(IterStack.FILTER_ITERATOR_PRI, "jaccsonFilter", "com.jaccson.server.TableScanIterator");
		TableScanIterator.setFilter(filterIterSetting, filter.toString());
		bscan.addScanIterator(filterIterSetting);
	}
}
