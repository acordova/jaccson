
package com.jaccson.server;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.IterStack;
import com.jaccson.JSONHelper;

/**
 * This selects subdocuments from JSON docs
 * 
 * This also is intended to only be used at scan time to avoid shipping all fields to
 * the client
 * 
 * @author aaron
 *
 */
public class SelectIterator  extends WrappingIterator implements OptionDescriber {

	JSONObject select;
	//private static final Logger log = Logger.getLogger(IteratorUtil.class);


	public SelectIterator() {}

	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		try {
			select = new JSONObject(options.get("select"));
		} catch (JSONException e) {
			throw new IOException(e);
		}
	}

	public SelectIterator(SortedKeyValueIterator<Key,Value> iterator) throws IOException {
		this.setSource(iterator);
	}


	public Value getTopValue() {

		Value v = super.getTopValue();

		JSONObject selected;

		try {
			JSONObject vo = new JSONObject(new String(v.get()));

			// perform select
			selected = select(vo, select);

			return new Value(selected.toString().getBytes());
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return v;
	}



	private static Map<String,Map<String,String>> parseOptions(Map<String,String> options) {
		HashMap<String,String> namesToClasses = new HashMap<String,String>();
		HashMap<String,Map<String,String>> namesToOptions = new HashMap<String,Map<String,String>>();
		HashMap<String,Map<String,String>> classesToOptions = new HashMap<String,Map<String,String>>();

		int index;
		String name;
		String subName;

		Collection<Entry<String,String>> entries = options.entrySet();
		for (Entry<String,String> e : entries) {
			name = e.getKey();
			if ((index = name.indexOf(".")) < 0)
				namesToClasses.put(name, e.getValue());
			else {
				subName = name.substring(0, index);
				if (!namesToOptions.containsKey(subName))
					namesToOptions.put(subName, new HashMap<String,String>());
				namesToOptions.get(subName).put(name.substring(index + 1), e.getValue());
			}
		}

		Collection<String> names = namesToClasses.keySet();
		for (String s : names) {
			classesToOptions.put(namesToClasses.get(s), namesToOptions.get(s));
		}

		return classesToOptions;
	}

	/*
	 * @Override public IteratorOptions requestOptions(ConsoleReader reader) throws IOException { String className; Map<String, String> options = new
	 * HashMap<String, String>();
	 * 
	 * String name = reader.readLine("The FilteringIterator removes entries using Filter classes\n" + "distinguishing name for FilteringIterator: ");
	 * 
	 * while (true) { className = reader.readLine("<filterClass> (hit enter to skip): "); if (className.length()==0) break; Class<? extends Filter> clazz; try {
	 * clazz = Accumulo.getClass(className); Filter f = clazz.newInstance(); if (!(f instanceof InteractiveOptionChooser)) throw new
	 * IOException(className+" does not implement an InteractiveOptionChooser, it is not configured to set properties via the shell");
	 * 
	 * IteratorOptions opt = ((InteractiveOptionChooser)f).requestOptions(reader); options.put(opt.name, className); for (Entry<String,String> entry :
	 * opt.options.entrySet()) options.put(opt.name+"."+entry.getKey(), entry.getValue()); } catch (ClassNotFoundException e) { throw new
	 * IOException("class not found: "+className); } catch (InstantiationException e) { throw new IOException("instantiation exception: "+className); } catch
	 * (IllegalAccessException e) { throw new IOException("illegal access exception: "+className); } }
	 * 
	 * return new IteratorOptions(name, options); } 
	 */ 


	@Override
	public IteratorOptions describeOptions() {
		return new IteratorOptions("select", "SelectIterator creates JSON subdocuments based on JSON 'select' documents", null,
				Collections.singletonList("<select JSON document>"));
	}

	@Override
	public boolean validateOptions(Map<String,String> options) {
		parseOptions(options);
		return true;
	}

	// called from client side
	public static void setSelect(IteratorSetting cfg, String select) {
		cfg.addOption("select", select);
	}

	public static void setSelectOnScanner(Scanner scanner, JSONObject select) {
		IteratorSetting selectIterSetting = new IteratorSetting(IterStack.SELECT_ITERATOR_PRI, "jaccsonSelecter", "com.jaccson.server.SelectIterator");
		SelectIterator.setSelect(selectIterSetting, select.toString());
		scanner.addScanIterator(selectIterSetting);
	}

	public static void setSelectOnScanner(BatchScanner bscan, JSONObject select) {
		IteratorSetting selectIterSetting = new IteratorSetting(IterStack.SELECT_ITERATOR_PRI, "jaccsonSelecter", "com.jaccson.server.SelectIterator");
		SelectIterator.setSelect(selectIterSetting, select.toString());
		bscan.addScanIterator(selectIterSetting);
	}

	public static void removeSelectOnScanner(Scanner scanner) {
		scanner.removeScanIterator("jaccsonSelecter");
	}
	
	public static JSONObject select(JSONObject original, JSONObject filter) throws JSONException {

		JSONObject selected = new JSONObject();

		for(String path : JSONObject.getNames(filter)) {
			subObjectForPath(path, original, selected);
		}

		return selected;
	}

	private static Object subObjectForPath(String path, Object o, Object sub) {

		if(o instanceof JSONArray) {
			JSONArray oa = (JSONArray)o;
			JSONArray subArray = (JSONArray)sub;

			for(int i=0; i < oa.length(); i++) {
				try {
					Object inner = oa.get(i);
					if(inner instanceof JSONObject) {
						if(i >= subArray.length())
							subArray.put(subObjectForPath(path, inner, new JSONObject()));
						else
							subObjectForPath(path, inner, subArray.get(i));
					}
					else {
						if(i >= subArray.length())
							subArray.put(subObjectForPath(path, inner, new JSONArray()));
						else
							subObjectForPath(path, inner, subArray.get(i));
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}

			return subArray;
		}
		else {

			// sub should be json object too
			JSONObject subObj = (JSONObject)sub;
			
			String[] steps = path.split("\\.");

			try {
				if(steps.length == 1) { // base case
					JSONObject jo = (JSONObject)o;
					if(!jo.has(steps[0]))
						return sub;
					subObj.put(steps[0], jo.get(steps[0]));
					return sub;
				}

				String subpath = JSONHelper.suffixPath(path);

				// recurse
				if(((JSONObject)o).has(steps[0])) {
					if(subObj.has(steps[0])) {
						subObj.put(steps[0], subObjectForPath(subpath, ((JSONObject)o).get(steps[0]), subObj.get(steps[0])));
					}
					else {
						Object inner = ((JSONObject)o).get(steps[0]);
						if(inner instanceof JSONObject)
							subObj.put(steps[0], subObjectForPath(subpath, inner, new JSONObject()));
						else
							subObj.put(steps[0], subObjectForPath(subpath, inner, new JSONArray()));
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}


			return sub;
		}
	}
}

