package com.jaccson.server;


import java.util.HashSet;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.log4j.Logger;
import org.bson.BSON;

import com.jaccson.mongo.BSONHelper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;


public class JaccsonUpdater extends Combiner {

	static final Logger log = Logger.getLogger(JaccsonUpdater.class);

	static DBObject deleteMarker = null;
	{
		deleteMarker = (DBObject) JSON.parse("{$delete:1}");
	}


	public enum operator {
		$inc, $set, $unset, $push, $pushAll,
		$addToSet, $each, $pop, $pull, $pullAll, 
		$rename, $bit, $delete
	}

	// TODO: raise an error if an array is not found
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void pullAll(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		BasicDBList valuesToPullj = (BasicDBList)object.get(path);
		HashSet valuesToPull = new HashSet();
		for(int i=0; i < valuesToPullj.size(); i++) {
			valuesToPull.add(valuesToPullj.get(i));
		}

		BasicDBList existingArray = (BasicDBList)o.get(field);
		BasicDBList newArray = new BasicDBList();

		// filter out values to pull from array
		for(int i=0; i < existingArray.size(); i++) {
			Object value = existingArray.get(i);
			if(valuesToPull.contains(value))
				continue;

			newArray.add(value);
		}

		// replace existing
		o.put(field, newArray);
	}

	// TODO: raise an error if an array is not found
	// note: integer 1 and float 1.0 don't compare as equal ...
	public static void pull(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Object valueToPull = object.get(path);

		BasicDBList existingArray = (BasicDBList)o.get(field);
		BasicDBList newArray = new BasicDBList();

		// filter out values to pull from array
		for(int i=0; i < existingArray.size(); i++) {
			Object value = existingArray.get(i);

			if(value.equals(valueToPull))
				continue;		

			newArray.add(value);
		}

		// replace existing
		o.put(field, newArray);

	}

	// TODO: raise an error if an array is not found
	// TODO: support popping first element using -1
	public static void pop(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		BasicDBList existingArray = (BasicDBList)o.get(field);
		BasicDBList newArray = new BasicDBList();

		// leave out the last
		for(int i=0; i < existingArray.size()-1; i++) {
			Object value = existingArray.get(i);
			newArray.add(value);
		}

		// replace existing
		o.put(field, newArray);

	}

	// TODO: raise an error if an array is not found
	// TODO: add $each support
	public static DBObject addToSet(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return object;
		}

		String path = object.keySet().iterator().next();

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		BasicDBList objectsToAdd = (BasicDBList)object.get(path);
		BasicDBList existingArray = (BasicDBList)o.get(field);
		BasicDBList newArray = new BasicDBList();

		HashSet<Object> values = new HashSet<Object>();
		for(int i=0; i < existingArray.size(); i++)
			values.add(existingArray.get(i));

		for(int i=0; i < objectsToAdd.size(); i++) 
			values.add(objectsToAdd.get(i));

		for(Object value : values)
			newArray.add(value);

		finalObj.removeField(field);
		finalObj.put(field, newArray);


		return finalObj;
	}

	// TODO: raise an error if an array is not found
	public static DBObject pushAll(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return object;
		}

		String path = object.keySet().iterator().next();

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		BasicDBList existingArray = (BasicDBList)o.get(field);
		BasicDBList valuesToAdd = (BasicDBList)object.get(path);

		for(int i=0; i < valuesToAdd.size(); i++) {
			Object value = valuesToAdd.get(i);

			existingArray.add(value);
		}

		return finalObj;
	}

	public static DBObject push(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return object;
		}

		String path = object.keySet().iterator().next();


		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		BasicDBList existingArray = (BasicDBList) o.get(field);
		Object value = object.get(path);

		existingArray.add(value);

		return finalObj;
	}

	public static void unset(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();


		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		o.removeField(field);
	}

	public static DBObject set(DBObject object, DBObject finalObj) {

		if(finalObj == null) {
			return object;
		}

		String path = object.keySet().iterator().next();

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		o.put(field, object.get(path));


		return finalObj;
	}

	public static DBObject increment(DBObject object, DBObject finalObj) {

		if(finalObj == null) {

			// may need to create subobjects
			DBObject o = new BasicDBObject();
			DBObject sub = o;

			for(String name : object.keySet().iterator().next().split("\\.")) {
				DBObject inner = new BasicDBObject();
				sub.put(name, inner);
				sub = inner;
			}

			return o;
		}

		String path = object.keySet().iterator().next();

		Integer amount = (Integer) object.get(path);

		DBObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Integer i = (Integer) o.get(field);
		i += amount;
		o.put(field, i);


		return finalObj;
	}


	public static DBObject applyUpdate(DBObject update, DBObject finalObj) {

		Iterator<String> keyIter = update.keySet().iterator();
		while(keyIter.hasNext()) {

			String op = update.keySet().iterator().next();

			log.info("applying update " + op + " to object " + finalObj);

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
				finalObj = increment((DBObject)update.get(op), finalObj);
				break;
			}
			case $set: {
				finalObj = set((DBObject)update.get(op), finalObj);
				break;
			}
			case $unset: {
				unset((DBObject)update.get(op), finalObj);
				break;
			}
			case $push: {
				finalObj = push((DBObject)update.get(op), finalObj);
				break;
			}
			case $pushAll: {
				finalObj = pushAll((DBObject)update.get(op), finalObj);
				break;
			}
			case $addToSet: {
				finalObj = addToSet((DBObject)update.get(op), finalObj);
				break;
			}
			case $pop: {
				pop((DBObject)update.get(op), finalObj);
				break;
			}
			case $pull: {
				pull((DBObject)update.get(op), finalObj);
				break;
			}
			case $pullAll: {
				pullAll((DBObject)update.get(op), finalObj);
				break;
			}
			case $rename: {
				//finalObj = rename((DBObject)next.get(fieldName), finalObj);
				break;
			}
			case $bit: {
				//bit((DBObject)next.get(fieldName), finalObj);
				break;
			}
			case $delete: {
				finalObj = null;
			}
			case $each: {
				// TODO: implement
				break;
			}
			}
		}

		return finalObj;
	}

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {

		DBObject finalObj = null; 

		// JaccsonTable inserts all mutations with reverse timestamps 
		// so we can apply these as we read them

		// TODO: if we have only one complete doc, avoid serializing and deserializing again
		while(iter.hasNext()) {

			try {
				DBObject next = (DBObject) BSON.decode(iter.next().get());

				log.info(next.toString());
				finalObj = applyUpdate(next, finalObj);

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


