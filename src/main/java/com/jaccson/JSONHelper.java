package com.jaccson;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONHelper {

	public static JSONObject innerMostObjectForPath(String path, JSONObject object) throws JSONException {

		String[] steps = path.split(".");

		for(int i=0; i < steps.length-1; i++) {
			String step = steps[i];

			// TODO: if an array is encountered, grab first object within?
			// or return an array of inner docs?
			object = object.getJSONObject(step);
		}

		return object;
	}

	public static String fieldFromPath(String path) {
		String[] steps = path.split(".");
		String field = steps[steps.length-1];
		return field;
	}

	// this is used to support select operations
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
				if(steps.length == 1) {
					JSONObject jo = (JSONObject)o;
					if(!jo.has(steps[0]))
						return sub;
					sub.put(steps[0], jo.get(steps[0]));
					return sub;
				}

				String subpath = "";
				for(int i=1; i < steps.length; i++)
					subpath += steps[i] + ".";

				// remove last .
				subpath = subpath.substring(0, subpath.length()-1);

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
