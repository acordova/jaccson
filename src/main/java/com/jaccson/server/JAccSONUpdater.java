package com.jaccson.server;


import java.util.HashSet;
import java.util.Iterator;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JSONHelper;


public class JAccSONUpdater implements Aggregator {

	private enum operator {
		$inc, $set, $unset, $push, $pushAll,
		$addToSet, $each, $pop, $pull, $pullAll, 
		$rename, $bit
	}

	private JSONObject latestUpdate;

	public void reset() {
		latestUpdate = null;
	}

	public void collect(Value value) {

		if(latestUpdate == null) {
			try {
				latestUpdate = new JSONObject(new String(value.get()));
			} catch (JSONException e) {
				// may get here because first 
				e.printStackTrace();
				return;
			}
		}

		try{
			JSONObject o = new JSONObject(new String(value.get()));

			@SuppressWarnings("rawtypes")
			Iterator keyIter = o.keys();
			while(keyIter.hasNext()) {
				String key = keyIter.next().toString();
				switch(operator.valueOf(key)) {
				case $inc: { 
					increment((JSONObject)o.get(key));
					break;
				}
				case $set: {
					set((JSONObject) o.get(key));
					break;
				}
				case $unset: {
					unset((JSONObject) o.get(key));
					break;
				}
				case $push: {
					push((JSONObject) o.get(key));
					break;
				}
				case $pushAll: {
					pushAll((JSONObject) o.get(key));
					break;
				}
				case $addToSet: {
					addToSet((JSONObject) o.get(key));
					break;
				}
				case $pop: {
					pop((JSONObject) o.get(key));
					break;
				}
				case $pull: {
					pull((JSONObject) o.get(key));
					break;
				}
				case $pullAll: {
					pullAll((JSONObject) o.get(key));
					break;
				}
				case $rename: {
					rename((JSONObject) o.get(key));
					break;
				}
				case $bit: {
					bit((JSONObject) o.get(key));
					break;
				}
				// basic insert / overwrite
				default: {
					latestUpdate = o;
					break;
				}
				}
			}

		}
		catch(JSONException e) {
			e.printStackTrace();
		}
	}

	private void bit(JSONObject object) {
		// TODO Auto-generated method stub

	}

	private void rename(JSONObject object) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void pullAll(JSONObject object) {
		if(latestUpdate == null) {
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
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

	private void pull(JSONObject object) {
		if(latestUpdate == null) {
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
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

	private void pop(JSONObject object) {
		if(latestUpdate == null) {
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
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

	private void addToSet(JSONObject object) {
		// TODO Auto-generated method stub

	}

	private void pushAll(JSONObject object) {
		if(latestUpdate == null) {
			latestUpdate = object;
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
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
	}

	private void push(JSONObject object) {
		if(latestUpdate == null) {
			latestUpdate = object;
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
			String field = JSONHelper.fieldFromPath(path);
			
			JSONArray existingArray = o.getJSONArray(field);
			Object value = object.getJSONArray(path);
			
			existingArray.put(value);
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void unset(JSONObject object) {
		if(latestUpdate == null) {
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
			String field = JSONHelper.fieldFromPath(path);
			
			o.remove(field);
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void set(JSONObject object) {
		if(latestUpdate == null) {
			latestUpdate = object;
			return;
		}

		String path = (String) object.keys().next();
		
		try {
			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
			String field = JSONHelper.fieldFromPath(path);
			
			o.put(field, object.get(path));
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void increment(JSONObject object) {

		if(latestUpdate == null) {
			latestUpdate = object;
			return;
		}

		String path = (String) object.keys().next();

		try {
			Integer amount = object.getInt(path);

			JSONObject o = JSONHelper.innerMostObjectForPath(path, latestUpdate);
			String field = JSONHelper.fieldFromPath(path);
			
			Integer i = o.getInt(field);
			i += amount;
			o.put(field, i);

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	

	public Value aggregate() {

		if(latestUpdate != null) {
			return new Value(latestUpdate.toString().getBytes());
		}

		return null;
	}

}
