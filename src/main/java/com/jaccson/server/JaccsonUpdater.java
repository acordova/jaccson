package com.jaccson.server;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import com.jaccson.mongo.BSONHelper;
import com.mongodb.util.JSON;

@SuppressWarnings("rawtypes")
public class JaccsonUpdater extends Combiner {

	static final Logger log = Logger.getLogger(JaccsonUpdater.class);

	static BasicBSONObject deleteMarker = null;
	{
		deleteMarker = new BasicBSONObject((Map)JSON.parse("{$delete:1}"));
	}


	public enum operator {
		$inc, $set, $unset, $push, $pushAll,
		$addToSet, $each, $pop, $pull, $pullAll, 
		$rename, $bit, $delete
	}

	// TODO: raise an error if an array is not found
	@SuppressWarnings("unchecked")
	public static void pullAll(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();

		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		
		Object[] valuesToPullj = (Object[]) object.get(path);
		HashSet valuesToPull = new HashSet();
		for(int i=0; i < valuesToPullj.length; i++) {
			valuesToPull.add(valuesToPullj[i]);
		}

		Object[] existingArray = (Object[]) o.get(field);
		BasicBSONList newArray = new BasicBSONList();

		// filter out values to pull from array
		for(int i=0; i < existingArray.length; i++) {
			Object value = existingArray[i];
			if(valuesToPull.contains(value))
				continue;

			newArray.add(value);
		}

		// replace existing
		o.put(field, newArray);
	}

	// TODO: raise an error if an array is not found
	// note: integer 1 and float 1.0 don't compare as equal ...
	public static void pull(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();

		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Object valueToPull = object.get(path);

		Object[] existingArray = (Object[])o.get(field);
		BasicBSONList newArray = new BasicBSONList();

		// filter out values to pull from array
		for(int i=0; i < existingArray.length; i++) {
			Object value = existingArray[i];

			if(value.equals(valueToPull))
				continue;		

			newArray.add(value);
		}

		// replace existing
		o.put(field, newArray);

	}

	// TODO: raise an error if an array is not found
	// TODO: support popping first element using -1
	public static void pop(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();

		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Object[] existingArray = (Object[])o.get(field);
		BasicBSONList newArray = new BasicBSONList();

		// leave out the last
		for(int i=0; i < existingArray.length-1; i++) {
			Object value = existingArray[i];
			newArray.add(value);
		}

		// replace existing
		o.put(field, newArray);

	}

	// TODO: raise an error if an array is not found
	// TODO: add $each support
	public static BasicBSONObject addToSet(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return new BasicBSONObject(object);
		}

		String path = object.keySet().iterator().next();

		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Object[] objectsToAdd = (Object[])object.get(path);
		Object[] existingArray = (Object[])o.get(field);
		BasicBSONList newArray = new BasicBSONList();

		HashSet<Object> values = new HashSet<Object>();
		for(int i=0; i < existingArray.length; i++)
			values.add(existingArray[i]);

		for(int i=0; i < objectsToAdd.length; i++) 
			values.add(objectsToAdd[i]);

		for(Object value : values)
			newArray.add(value);

		o.remove(field);
		o.put(field, newArray);


		return finalObj;
	}

	// TODO: raise an error if an array is not found
	public static BasicBSONObject pushAll(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return new BasicBSONObject(object);
		}

		String path = object.keySet().iterator().next();

		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Object[] objectsToAdd = (Object[])object.get(path);
		Object[] existingArray = (Object[])o.get(field);
		BasicBSONList newArray = new BasicBSONList();

		HashSet<Object> values = new HashSet<Object>();
		for(int i=0; i < existingArray.length; i++)
			values.add(existingArray[i]);

		for(int i=0; i < objectsToAdd.length; i++) 
			values.add(objectsToAdd[i]);

		for(Object value : values)
			newArray.add(value);

		o.remove(field);
		o.put(field, newArray);
		
		return finalObj;
	}

	public static BasicBSONObject push(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return new BasicBSONObject(object);
		}

		String path = object.keySet().iterator().next();


		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Object[] existingArray = (Object[]) o.get(field);
		BasicBSONList newArray = new BasicBSONList();
		
		for(int i=0; i < existingArray.length; i++)
			newArray.add(existingArray[i]);
		
		Object value = object.get(path);
		newArray.add(value);
		o.put(field, newArray);
		
		return finalObj;
	}

	public static void unset(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return;
		}

		String path = object.keySet().iterator().next();


		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		o.remove(field);
	}

	public static BasicBSONObject set(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {
			return new BasicBSONObject(object);
		}

		String path = object.keySet().iterator().next();

		BasicBSONObject o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		o.put(field, object.get(path));


		return finalObj;
	}

	public static BasicBSONObject increment(Map<String,Object> object, BasicBSONObject finalObj) {

		if(finalObj == null) {

			// may need to create subobjects
			BasicBSONObject o = new BasicBSONObject();
			BasicBSONObject sub = o;

			for(String name : object.keySet().iterator().next().split("\\.")) {
				BasicBSONObject inner = new BasicBSONObject();
				sub.put(name, inner);
				sub = inner;
			}

			return o;
		}

		String path = object.keySet().iterator().next();

		Long amount = (Long) object.get(path);

		Map<String,Object> o = BSONHelper.innerMostObjectForPath(path, finalObj);
		String field = BSONHelper.fieldFromPath(path);

		Long i = (Long) o.get(field);
		i += amount;
		o.put(field, i);


		return finalObj;
	}


	@SuppressWarnings("unchecked")
	public static BasicBSONObject applyUpdate(BasicBSONObject next, BasicBSONObject finalObj) {

		
		Iterator<String> keyIter = next.keySet().iterator();
		
		while(keyIter.hasNext()) {

			String op = keyIter.next();

			log.info("applying update " + op + " to object " + finalObj);

			operator x;
			try {
				x = operator.valueOf(op);
			}
			catch (IllegalArgumentException iae){
				log.info("simple overwrite");
				finalObj = next;
				break;
			}

			log.info(x.toString());

			// updates should contain at least one update operator ...		
			switch(x) {
			case $inc: { 
				finalObj = increment((Map<String,Object>) next.get(op), finalObj);
				break;
			}
			case $set: {
				finalObj = set((Map<String,Object>) next.get(op), finalObj);
				break;
			}
			case $unset: {
				unset((Map<String,Object>) next.get(op), finalObj);
				break;
			}
			case $push: {
				finalObj = push((Map<String,Object>)next.get(op), finalObj);
				break;
			}
			case $pushAll: {
				finalObj = pushAll((Map<String,Object>) next.get(op), finalObj);
				break;
			}
			case $addToSet: {
				finalObj = addToSet((Map<String,Object>)next.get(op), finalObj);
				break;
			}
			case $pop: {
				pop((Map<String,Object>)next.get(op), finalObj);
				break;
			}
			case $pull: {
				pull((Map<String, Object>) next.get(op), finalObj);
				break;
			}
			case $pullAll: {
				pullAll((Map<String,Object>)next.get(op), finalObj);
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

		BasicBSONObject finalObj = null; 

		// JaccsonTable inserts all mutations with reverse timestamps 
		// so we can apply these as we read them

		// TODO: if we have only one complete doc, avoid serializing and deserializing again
		while(iter.hasNext()) {

			try {
				BasicBSONObject next = (BasicBSONObject) BSON.decode(iter.next().get());

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


