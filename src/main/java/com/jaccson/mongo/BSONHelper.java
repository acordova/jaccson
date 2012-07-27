


package com.jaccson.mongo;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import org.bson.BSON;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class BSONHelper {

	// used for deserializing key value pairs from tables into DBObjects
	public static DBObject objectForEntry(Entry<Key,Value> pair) {
		return objectForKeyValue(pair.getKey(), pair.getValue());
	}

	public static DBObject objectForKeyValue(Key k, Value v) {
		DBObject o = null;

		o = (DBObject)BSON.decode(v.get());

		// add key 
		o.put("_id", new ObjectId(k.getRow().toString()));

		return (DBObject)o;
	}


	// TODO: does this need to handle arrays ? 
	public static DBObject innerMostObjectForPath(String path, DBObject object)  {

		String[] steps = path.split("\\.");

		for(int i=0; i < steps.length-1; i++) {
			String step = steps[i];

			// TODO: if an array is encountered, grab first object within?
			// or return an array of inner docs?
			object = (DBObject) object.get(step);
		}

		return object;
	} 

	/**
	 * 
	 * 	this is used for matching docs on non-indexed fields
	 * 
	 * @param path
	 * @param object
	 * @return
	 */
	public static Object valueForPath(String path, DBObject object) {
		String[] steps = path.split("\\.");

		ArrayList<Object> results = new ArrayList<Object>();

		valuesForPath(steps, object, results);

		if(results.size() == 0)
			return null;

		if(results.size() == 1)
			return results.get(0);

		return results;
	}

	private static void valuesForPath(String[] steps, Object object, ArrayList<Object> results) {

		if(object instanceof DBObject) {

			// base case
			if(steps.length == 1) {
				results.add(((DBObject)object).get(steps[0]));
				return;
			}

			String[] remainingSteps = new String[steps.length-1];
			for(int i=1; i < steps.length; i++)
				remainingSteps[i-1] = steps[i];

			valuesForPath(remainingSteps, ((DBObject) object).get(steps[0]), results);
		}
		else {

			BasicDBList ia = (BasicDBList)object;

			for(int i=0; i < ia.size(); i++) {
				valuesForPath(steps, ia.get(i), results);
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

		if(o instanceof BasicDBList) {
			BasicDBList oa = (BasicDBList)o;
			BasicDBList subArray = new BasicDBList();

			for(int i=0; i < oa.size(); i++) {
				subArray.add(subObjectForPath(path, oa.get(i)));
			}

			return subArray;
		}
		else {

			DBObject sub = new BasicDBObject();

			String[] steps = path.split("\\.");


			if(steps.length == 1) { // base case
				DBObject jo = (DBObject)o;
				if(!jo.containsField(steps[0]))
					return sub;
				sub.put(steps[0], jo.get(steps[0]));
				return sub;
			}

			String subpath = suffixPath(path);

			// recurse
			if(((DBObject)o).containsField(steps[0]))
				sub.put(steps[0], subObjectForPath(subpath, ((DBObject)o).get(steps[0])));

			return sub;
		}
	}
}


