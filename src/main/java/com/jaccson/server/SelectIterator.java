
package com.jaccson.server;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JSONHelper;
import com.jaccson.SelectFilter;

/**
 * note that this doesn't work like an Accumulo filter, which in our case would filter
 * out entire JSON object, rather this filter suppresses unselected fields within each 
 * JSON object
 * 
 * @author aaron
 *
 */
public class SelectIterator  extends WrappingIterator implements OptionDescriber {
	
	JSONObject filter;

	//private static final Logger log = Logger.getLogger(IteratorUtil.class);


	public SelectIterator() {}

	public SelectIterator(SortedKeyValueIterator<Key,Value> iterator) throws IOException {
		this.setSource(iterator);

	}


	public Value getTopValue() {
		
		Value v = super.getTopValue();
		
		JSONObject selected;
		
		try {
			JSONObject vo = new JSONObject(new String(v.get()));
			
			// perform select
			selected = SelectFilter.select(vo, filter);
			
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
	 

	//@Override
	public IteratorOptions describeOptions() {
		return new IteratorOptions("filter", "JASelectFilter creates JSON subdocuments based on JSON 'select' documents", null,
				Collections.singletonList("<select JSON document>"));
	}

	//@Override
	public boolean validateOptions(Map<String,String> options) {
		parseOptions(options);
		return true;
	}


}

