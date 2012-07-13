package com.jaccson.server;


import java.util.HashSet;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JSONHelper;


public class JaccsonUpdater extends Combiner {

	static final Logger log = Logger.getLogger(JaccsonUpdater.class);
	
	static JSONObject deleteMarker = null;
	{
		try {
			deleteMarker = new JSONObject("{$delete:1}");
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	public enum operator {
		$inc, $set, $unset, $push, $pushAll,
		$addToSet, $each, $pop, $pull, $pullAll, 
		$rename, $bit, $delete
	}

	// TODO: raise an error if an array is not found
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void pullAll(JSONObject object, JSONObject finalObj) {
		
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
			for(int i=0; i < existingArray.length(); i++) {
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

	// TODO: raise an error if an array is not found
	// note: integer 1 and float 1.0 don't compare as equal ...
	public static void pull(JSONObject object, JSONObject finalObj) {

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
			for(int i=0; i < existingArray.length(); i++) {
				Object value = existingArray.get(i);

				if(value.equals(valueToPull))
					continue;		

				newArray.put(value);
			}

			// replace existing
			o.put(field, newArray);

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	// TODO: raise an error if an array is not found
	// TODO: support popping first element using -1
	public static void pop(JSONObject object, JSONObject finalObj) {
		
		if(finalObj == null) {
			return;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray existingArray = o.getJSONArray(field);
			JSONArray newArray = new JSONArray();

			// leave out the last
			for(int i=0; i < existingArray.length()-1; i++) {
				Object value = existingArray.get(i);
				newArray.put(value);
			}

			// replace existing
			o.put(field, newArray);

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	// TODO: raise an error if an array is not found
	// TODO: add $each support
	public static JSONObject addToSet(JSONObject object, JSONObject finalObj) {
		
		if(finalObj == null) {
			return object;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray objectsToAdd = object.getJSONArray(path);
			JSONArray existingArray = o.getJSONArray(field);
			JSONArray newArray = new JSONArray();

			HashSet<Object> values = new HashSet<Object>();
			for(int i=0; i < existingArray.length(); i++)
				values.add(existingArray.get(i));

			for(int i=0; i < objectsToAdd.length(); i++) 
				values.add(objectsToAdd.get(i));

			for(Object value : values)
				newArray.put(value);

			finalObj.remove(field);
			finalObj.put(field, newArray);

		} catch (JSONException e) {
			e.printStackTrace();
		}

		return finalObj;
	}

	// TODO: raise an error if an array is not found
	public static JSONObject pushAll(JSONObject object, JSONObject finalObj) {
		
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

	public static JSONObject push(JSONObject object, JSONObject finalObj) {
		
		if(finalObj == null) {
			return object;
		}

		String path = (String) object.keys().next();

		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, finalObj);
			String field = JSONHelper.fieldFromPath(path);

			JSONArray existingArray = o.getJSONArray(field);
			Object value = object.get(path);

			existingArray.put(value);

		} catch (JSONException e) {
			e.printStackTrace();
		}

		return finalObj;
	}

	public static void unset(JSONObject object, JSONObject finalObj) {
		
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

	public static JSONObject set(JSONObject object, JSONObject finalObj) {
		
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

	public static JSONObject increment(JSONObject object, JSONObject finalObj) {

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


	public static JSONObject applyUpdate(JSONObject update, JSONObject finalObj) throws JSONException {

		@SuppressWarnings("rawtypes")
		Iterator keyIter = update.keys();
		while(keyIter.hasNext()) {

			String op = keyIter.next().toString();

			operator x;
			try {
				x = operator.valueOf(op);
			}
			catch (IllegalArgumentException iae){
				log.info("simple overwrite");
				finalObj = update;
				break;
			}
					
			log.info(x.toString());
			
			// updates should contain at least one update operator ...		
			switch(x) {
			case $inc: { 
				finalObj = increment((JSONObject)update.get(op), finalObj);
				break;
			}
			case $set: {
				finalObj = set((JSONObject)update.get(op), finalObj);
				break;
			}
			case $unset: {
				unset((JSONObject)update.get(op), finalObj);
				break;
			}
			case $push: {
				finalObj = push((JSONObject)update.get(op), finalObj);
				break;
			}
			case $pushAll: {
				finalObj = pushAll((JSONObject)update.get(op), finalObj);
				break;
			}
			case $addToSet: {
				finalObj = addToSet((JSONObject)update.get(op), finalObj);
				break;
			}
			case $pop: {
				pop((JSONObject)update.get(op), finalObj);
				break;
			}
			case $pull: {
				pull((JSONObject)update.get(op), finalObj);
				break;
			}
			case $pullAll: {
				pullAll((JSONObject)update.get(op), finalObj);
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
			case $delete: {
				finalObj = null;
			}
			}
		}

		return finalObj;
	}

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {

		JSONObject finalObj = null; 

		// JaccsonTable inserts all mutations with reverse timestamps 
		// so we can apply these as we read them
		
		// TODO: if we have only one complete doc, avoid serializing and deserializing again
		while(iter.hasNext()) {
			
			try {
				JSONObject next = new JSONObject(new String(iter.next().get()));
				
				log.info(next.toString());
				finalObj = applyUpdate(next, finalObj);

			}
			catch(JSONException je) {
				// can happen if an array is not found, for example ...
				log.info(je.getMessage());
			}
			catch (IllegalArgumentException iae) {
				// TODO: handle this
				log.info(iae.getMessage());
			}
		}

		if(finalObj == null) {
			finalObj = deleteMarker;
		}
		
		return new Value(finalObj.toString().getBytes());
	}
}


