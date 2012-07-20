package com.jaccson;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SelectFilter {

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
