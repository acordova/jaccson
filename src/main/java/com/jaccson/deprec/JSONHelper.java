package com.jaccson.deprec;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONHelper {

	// used for deserializing key value pairs from tables into JSONObjects
	public static JSONObject objectForEntry(Entry<Key,Value> pair) {
		return objectForKeyValue(pair.getKey(), pair.getValue());
	}
	
	public static JSONObject objectForKeyValue(Key k, Value v) {
		JSONObject o = null;
		try {
			o = new JSONObject(new String(v.get()));
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// add key 
		try {
			o.put("_id", k.getRow().toString());
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return o;
	}
	
	
	// TODO: does this need to handle arrays ? 
	public static JSONObject innerMostObjectForPath(String path, JSONObject object) throws JSONException {

		String[] steps = path.split("\\.");

		for(int i=0; i < steps.length-1; i++) {
			String step = steps[i];

			// TODO: if an array is encountered, grab first object within?
			// or return an array of inner docs?
			object = object.getJSONObject(step);
		}

		return object;
	} 
	
	/**
	 * throws JSONException if a field is not found
	 * 
	 * 	this is used for matching docs on non-indexed fields
	 * 
	 * @param path
	 * @param object
	 * @return
	 * @throws JSONException
	 */
	public static Object valueForPath(String path, JSONObject object) throws JSONException {
		String[] steps = path.split("\\.");
		
		ArrayList<Object> results = new ArrayList<Object>();
		
		valuesForPath(steps, object, results);
		
		if(results.size() == 0)
			return null;
					
		if(results.size() == 1)
			return results.get(0);
		
		return results;
	}
	
	private static void valuesForPath(String[] steps, Object object, ArrayList<Object> results) throws JSONException {

		if(object instanceof JSONObject) {
			
			// base case
			if(steps.length == 1) {
				results.add(((JSONObject)object).get(steps[0]));
				return;
			}
			
			String[] remainingSteps = new String[steps.length-1];
			for(int i=1; i < steps.length; i++)
				remainingSteps[i-1] = steps[i];
			
			valuesForPath(remainingSteps, ((JSONObject) object).get(steps[0]), results);
		}
		else {
		
			JSONArray ia = (JSONArray)object;
			
			for(int i=0; i < ia.length(); i++) {
				try {
					valuesForPath(steps, ia.get(i), results);
				}
				catch(JSONException e) {
					
				}
			}	
		}
	}

	public static String fieldFromPath(String path) {
		String[] steps = path.split("\\.");
		String field = steps[steps.length-1];
		return field;
	}

	
	// these methods are used to support select operations

	
	// remove first .-separated component
	public static String suffixPath(String path) {
		
		String[] steps = path.split("\\.");
		
		String subpath = "";
		for(int i=1; i < steps.length; i++)
			subpath += steps[i] + ".";

		// remove last .
		subpath = subpath.substring(0, subpath.length()-1);

		return subpath;
	}
	
	public static Object subObjectForPath(String path, Object o) {

		if(o instanceof JSONArray) {
			JSONArray oa = (JSONArray)o;
			JSONArray subArray = new JSONArray();

			for(int i=0; i < oa.length(); i++) {
				try {
					subArray.put(subObjectForPath(path, oa.get(i)));
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}

			return subArray;
		}
		else {

			JSONObject sub = new JSONObject();

			String[] steps = path.split("\\.");

			try {
				if(steps.length == 1) { // base case
					JSONObject jo = (JSONObject)o;
					if(!jo.has(steps[0]))
						return sub;
					sub.put(steps[0], jo.get(steps[0]));
					return sub;
				}

				String subpath = suffixPath(path);

				// recurse
				if(((JSONObject)o).has(steps[0]))
					sub.put(steps[0], subObjectForPath(subpath, ((JSONObject)o).get(steps[0])));
			} catch (JSONException e) {
				e.printStackTrace();
			}


			return sub;
		}
	}
}
