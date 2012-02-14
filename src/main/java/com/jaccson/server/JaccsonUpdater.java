package com.jaccson.server;


import java.util.HashSet;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JSONHelper;


public class JaccsonUpdater extends Combiner {

	private enum operator {
		$inc, $set, $unset, $push, $pushAll,
		$addToSet, $each, $pop, $pull, $pullAll, 
		$rename, $bit
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void pullAll(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return;
		}
		
		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray valuesToPullj = object.getJSONArray(path);
			HashSet valuesToPull = new HashSet();
			for(int i=0; i < valuesToPullj.length(); i++) {
				valuesToPull.add(valuesToPullj.get(i));
			}

			JSONArray existingArray = o.getJSONArray(field);
			JSONArray newArray = new JSONArray();

			// filter out values to pull from array
			for(int i=0; i < existingArray.length() -1; i++) {
				Object value = existingArray.get(i);
				if(valuesToPull.contains(value))
					continue;

				newArray.put(value);
			}

			// replace existing
			o.put(field, newArray);

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	private static void pull(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return;
		}
		
		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			Object valueToPull = object.get(path);

			JSONArray existingArray = o.getJSONArray(field);
			JSONArray newArray = new JSONArray();

			// filter out values to pull from array
			for(int i=0; i < existingArray.length() -1; i++) {
				Object value = existingArray.get(i);
				if(value instanceof String) {
					if(value.equals(valueToPull))
						continue;
				}
				else {
					if(value == valueToPull)
						continue;
				}

				newArray.put(value);
			}

			// replace existing
			o.put(field, newArray);

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	private static void pop(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray existingArray = o.getJSONArray(field);
			JSONArray newArray = new JSONArray();
			for(int i=0; i < existingArray.length() -1; i++) {
				Object value = existingArray.get(i);
				newArray.put(value);
			}

			// replace existing
			o.put(field, newArray);

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	private static JSONObject addToSet(JSONObject object, JSONObject finalObj) {
		// TODO Auto-generated method stub
		return null;

	}

	private static JSONObject pushAll(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return object;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray existingArray = o.getJSONArray(field);
			JSONArray valuesToAdd = object.getJSONArray(path);

			for(int i=0; i < valuesToAdd.length(); i++) {
				Object value = valuesToAdd.get(i);

				existingArray.put(value);
			}

		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return finalObj;
	}

	private static JSONObject push(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return object;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray existingArray = o.getJSONArray(field);
			Object value = object.getJSONArray(path);

			existingArray.put(value);

		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return finalObj;
	}

	private static void unset(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			o.remove(field);

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private static JSONObject set(JSONObject object, JSONObject finalObj) {
		if(finalObj == null) {
			return object;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			o.put(field, object.get(path));

		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return finalObj;
	}

	private static JSONObject increment(JSONObject object, JSONObject finalObj) {

		if(finalObj == null) {
			return object;
		}

		String path = (String) object.keys().next();

		try {
			Integer amount = object.getInt(path);

			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			Integer i = o.getInt(field);
			i += amount;
			o.put(field, i);

		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return finalObj;
	}




	@Override
	public Value reduce(Key key, Iterator<Value> iter) {

		JSONObject finalObj = null; 

		// handle initial key - if regular value or update + upsert ...

		while(iter.hasNext()) {

			try {

				JSONObject next = new JSONObject(new String(iter.next().get()));

				@SuppressWarnings("rawtypes")
				Iterator keyIter = next.keys();
				while(keyIter.hasNext()) {

					String fieldName = keyIter.next().toString();


					// updates should contain at least one update operator ...		
					switch(operator.valueOf(fieldName)) {
					case $inc: { 
						finalObj = increment((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $set: {
						finalObj = set((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $unset: {
						unset((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $push: {
						finalObj = push((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $pushAll: {
						finalObj = pushAll((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $addToSet: {
						finalObj = addToSet((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $pop: {
						pop((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $pull: {
						pull((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $pullAll: {
						pullAll((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $rename: {
						//finalObj = rename((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					case $bit: {
						//bit((JSONObject)next.get(fieldName), finalObj);
						break;
					}
					// basic insert / overwrite
					default: {
						finalObj = next;
						break;
					}
					}

				}
			}
			catch(JSONException je) {
				// should not happen, client should disallow invalid json
			}
		}

		return new Value(finalObj.toString().getBytes());
	}
}
