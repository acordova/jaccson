package com.jaccson;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.mortbay.util.ajax.JSON;

import com.jaccson.server.DeletedFilter;
import com.jaccson.server.JaccsonUpdater;
import com.jaccson.server.JaccsonUpdater.operator;
import com.jaccson.server.SelectIterator;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoder;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;


public class DBCollection {

	private String name;
	private DB db;
	private Set<String> indexedKeys;
	private TableMetadata metadata;
	String tableName;
	private boolean dirty;
	Connector conn;
	Authorizations auths;
	private BatchWriter writer;
	private HashMap<String, BatchWriter> indexWriters;
	private Scanner simpleGetter;

	private static final Value BLANK_VALUE = new Value("".getBytes());
	private static WriteResult writeResult;


	public DBCollection(String name, DB db) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
		this.name = name;
		this.db = db;
		this.tableName = db.name + "_" + name;
		this.conn = db.jaccson.conn;
		this.auths = db.jaccson.auths;

		metadata = new TableMetadata(conn, auths);

		indexedKeys = metadata.getIndexedKeys(tableName); 
		dirty = false;

		writeResult = new WriteResult(null, null);
	}


	private void getWritersReaders() throws TableNotFoundException {

		if(writer != null)
			return;

		writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 10);
		indexWriters = new HashMap<String,BatchWriter>();
		simpleGetter = conn.createScanner(tableName, auths);
		simpleGetter.setBatchSize(1);
	}

	private BatchWriter writerForIndexKey(String key) throws TableNotFoundException {

		if(!indexWriters.containsKey(key)) {
			indexWriters.put(key, conn.createBatchWriter(tableNameForIndex(key), 1000000L, 1000L, 10));
		}

		return indexWriters.get(key);
	}

	private void indexBSON(Object obj, String prefix, String rowid) throws MutationsRejectedException, TableNotFoundException {
		indexBSON( obj,  prefix,  rowid, null, false);
	}

	private void indexBSON(Object obj, String prefix, String rowid, ColumnVisibility cv) throws MutationsRejectedException, TableNotFoundException {
		indexBSON(obj, prefix, rowid, cv, false);
	}

	private void indexBSON(Object obj, String prefix, String rowid, ColumnVisibility cv, boolean delete) throws MutationsRejectedException, TableNotFoundException {

		if(obj instanceof DBObject) {

			DBObject jobj = (DBObject) obj;

			for(String name : jobj.keySet()) {
				Object o = jobj.get(name);

				indexBSON(o, prefix + name + ".", rowid, cv, delete);
			}
		}

		else if (obj instanceof BasicDBList) {

			BasicDBList oarr = (BasicDBList)obj;

			// index under the same name?
			for(int i=0; i < oarr.size(); i++) {
				indexBSON(oarr.get(i), prefix, rowid, cv, delete);
			}
		}

		else if (obj instanceof Double || obj instanceof Integer || obj instanceof String) {

			if(prefix.endsWith("."))
				prefix = prefix.substring(0, prefix.length() -1);

			// check to see whether this field is indexed
			if(!indexedKeys.contains(prefix)) {
				return;
			}

			BatchWriter indexWriter = writerForIndexKey(prefix);

			if(obj instanceof String) {
				// index individual words
				HashSet<String> words = new HashSet<String>();
				for(String s : ((String) obj).split("\\s+")) {
					words.add(s.toLowerCase());
				}

				for(String word: words) {
					byte[] bytes = IndexHelper.indexValueForObject(word);

					Mutation m = new Mutation(new Text(bytes));
					if(delete) {
						if(cv == null)
							m.putDelete(tableName + "_" + prefix, rowid);
						else
							m.putDelete(tableName + "_" + prefix, rowid, cv);
					}
					else {
						if(cv == null)
							m.put(tableName + "_" + prefix, rowid, BLANK_VALUE);
						else
							m.put(tableName + "_" + prefix, rowid, cv, BLANK_VALUE);
					}

					indexWriter.addMutation(m);
				}
			}
			else {
				byte[] bytes = IndexHelper.indexValueForObject(obj);

				Mutation m = new Mutation(new Text(bytes));
				if(delete) {
					if(cv == null)
						m.putDelete(tableName + "_" + prefix, rowid);
					else
						m.putDelete(tableName + "_" + prefix, rowid, cv);
				}
				else {
					if(cv == null)
						m.put(tableName + "_" + prefix, rowid, BLANK_VALUE);
					else
						m.put(tableName + "_" + prefix, rowid, cv, BLANK_VALUE);
				}

				indexWriter.addMutation(m);
			}
		}
	}

	private String tableNameForIndex(String path) {
		return tableName + "_" + path.replace('.', '_');	
	}

	public WriteResult insert(DBObject[] arr, WriteConcern concern)
			throws MongoException {

		return insert(arr);
	}


	public WriteResult insert(DBObject[] arr, WriteConcern concern,
			DBEncoder encoder) throws MongoException {

		return insert(arr);
	}


	/**
	 * concern is ignored
	 * 
	 * by default Accumulo sends all writes to a replicated write ahead log
	 * errors are raised for network and server errors
	 * 
	 * @param obj
	 * @param concern
	 * @return
	 * @throws MongoException
	 */
	public WriteResult insert(DBObject obj, WriteConcern concern)
			throws MongoException {

		// extract col vis
		String cvstring = null;
		if(obj.containsField("$security"))
			cvstring = (String)obj.get("$security");

		ColumnVisibility cv = null;
		if(cvstring != null)
			cv = new ColumnVisibility(cvstring);


		try {
			getWritersReaders();
		} catch (TableNotFoundException e) {
			throw new MongoException(e.getLocalizedMessage());
		}

		Object objid = null;
		String rowid = null;
		if(obj.containsField("_id")) {
			objid = obj.get("_id");

			if(objid instanceof String)
				rowid = (String)objid;
			else if(objid instanceof ObjectId)
				rowid = ((ObjectId)objid).toString() + "_o";

			obj.removeField("_id");
		}
		else {
			// generate ID
			rowid = new ObjectId().toString() + "_o"; //UUID.randomUUID().toString() + "_o";
		}

		Mutation m = new Mutation(rowid);
		if(cv == null)
			m.put("BSON", "", Long.MAX_VALUE - System.currentTimeMillis(), new Value(BSON.encode(obj)));
		else
			m.put("BSON", "", cv, Long.MAX_VALUE - System.currentTimeMillis(), new Value(BSON.encode(obj)));

		try {
			writer.addMutation(m);

			// might want to flush writer here, in case index is written before main mutation

			// if no indexed keys, skip
			if(indexedKeys.size() > 0)
				indexBSON(obj, "", rowid, cv);

		} catch (Exception e) {
			throw new MongoException(e.getLocalizedMessage());
		}

		dirty = true;

		// put the ID back
		obj.put("_id", objid);

		// TODO: update count

		writeResult.setN(1);
		return writeResult;
	}

	@SuppressWarnings("unchecked")
	public WriteResult insert(String json) {
		
		return insert(new BasicDBObject((Map<String,Object>)JSON.parse(json)), null);
	}
	
	public WriteResult insert(DBObject... arr) throws MongoException {

		for(DBObject obj : arr) {
			insert(obj, null);
		} 

		writeResult.setN(arr.length);
		return writeResult;
	}


	public WriteResult insert(WriteConcern concern, DBObject... arr)
			throws MongoException {

		return insert(arr);
	}


	public WriteResult insert(List<DBObject> list) throws MongoException {

		for(DBObject obj : list) {
			insert(obj, null);
		} 

		writeResult.setN(list.size());
		return writeResult;
	}


	public WriteResult insert(List<DBObject> list, WriteConcern concern)
			throws MongoException {

		return insert(list);
	}


	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi, WriteConcern concern) throws MongoException {

		return update(q, o, false, multi, concern, null);
	}


	public WriteResult update(DBObject q, DBObject modsj, boolean upsert,
			boolean multi, WriteConcern concern, DBEncoder encoder)
					throws MongoException {

		try {
			getWritersReaders();
		} catch (TableNotFoundException e) {
			throw new MongoException(e.getLocalizedMessage());
		}

		int affected = 0;

		DBCursor cur = find(q, null);
		boolean found = false;
		for(DBObject o : cur) {
			found = true;

			String objid = ((ObjectId)o.get("_id")).toString();
			Mutation m = new Mutation(objid);

			@SuppressWarnings("rawtypes")
			// break update into several objects
			Iterator keyIter = modsj.keySet().iterator();
			while(keyIter.hasNext()) {
				DBObject mod = new BasicDBObject();
				String command = (String) keyIter.next();

				// verify command is legal
				// throws IllegalArgumentException
				JaccsonUpdater.operator.valueOf(command);

				mod.put(command, modsj.get(command));
				m.put("BSON", "", Long.MAX_VALUE - System.currentTimeMillis(), new Value(BSON.encode(mod)));
				try {
					writer.addMutation(m);

					// update indexes
					indexUpdate(mod, objid);
				} catch (Exception e) {
					throw new MongoException(e.getLocalizedMessage());
				}

				dirty = true;

				affected++;
			}

			if(!multi)
				break;
		}

		// TODO: review this whole thing
		if(!found && upsert) {

			DBObject obj = new BasicDBObject();
			for(String path : obj.keySet()) {
				Object val = obj.get(path);
				DBObject sub = obj;

				String[] parts = path.split(".");
				for(int i=0; i < parts.length - 1; i++) {
					DBObject inner = new BasicDBObject();
					sub.put(parts[i], inner);
					sub = inner;
				}

				sub.put(parts[parts.length-1], val);
			}

			@SuppressWarnings("rawtypes")
			Iterator kiter = modsj.keySet().iterator();

			while(kiter.hasNext()) {
				String key = (String) kiter.next();
				obj.put(key, modsj.get(key));
			}

			insert(obj);
			affected++;
			dirty = true;
		}

		writeResult.setN(affected);
		return writeResult;
	}


	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi) throws MongoException {

		return update(q, o, false, multi, null, null);
	}

	public WriteResult update(String q, String o) {
		return update(new BasicDBObject((Map)JSON.parse(q)), new BasicDBObject((Map)JSON.parse(o)));
	}
	
	public WriteResult update(DBObject q, DBObject o) throws MongoException {

		return update(q, o, false, false, null, null);
	}


	public WriteResult updateMulti(DBObject q, DBObject modsj)
			throws MongoException {

		return update(q, modsj, false, true, null, null);
	}

	private void indexUpdate(DBObject mod, String rowID) throws MutationsRejectedException, TableNotFoundException {

		String command = (String) mod.keySet().iterator().next();
		DBObject update = (DBObject) mod.get(command);
		String prefix = (String)update.keySet().iterator().next();
		Object obj = update.get(prefix);

		operator op = operator.valueOf(command);

		switch(op) {
		case $inc:
		{
			// retrieve, remove, insert?
		}
		case $set:
		{
			indexBSON(obj, prefix, rowID);
			break;
		}
		case $unset:
		{
			indexBSON(obj, prefix, rowID, null, true);
			break;
		}
		case $push:
		{
			indexBSON(obj, prefix, rowID);
			break;
		}
		case $pushAll:
		{
			indexBSON(obj, prefix, rowID);
			break;
		}
		case $addToSet:
		{
			indexBSON(obj, prefix, rowID);
			break;
		}
		case $each:
		{
			break;
		}
		case $pop:
		{
			// retrieve, remove from index
			break;
		}
		case $pull:
		{
			indexBSON(obj, prefix, rowID, null, true);
			break;
		}
		case $pullAll:
		{
			indexBSON(obj, prefix, rowID, null, true);
			break;
		}
		case $rename:
		{
			// TODO: implement
			break;
		}
		case $bit:
		{
			// TODO: implement
			break;
		}
		case $delete: {
			// ignore? .. users shouldn't use this ...
			break;
		}

		}
	}


	public WriteResult remove(DBObject o, WriteConcern concern)
			throws MongoException {

		return remove(o);
	}


	public WriteResult remove(DBObject o, WriteConcern concern,
			DBEncoder encoder) throws MongoException {

		return remove(o);
	}

	public WriteResult remove(String query) {
		return remove(new BasicDBObject((Map)JSON.parse(query)));
	}
	
	public WriteResult remove(DBObject query) throws MongoException {
		
		try {
			getWritersReaders();
		} catch (TableNotFoundException e) {
			throw new MongoException(e.getLocalizedMessage());
		}

		String rowid = ((ObjectId)query.get("_id")).toString();

		// just delete this one object
		if(rowid != null) { // TODO: look at any other clauses? warn user that they're ignored?
			Mutation m = new Mutation(rowid);
			m.put("BSON", "", Long.MAX_VALUE - System.currentTimeMillis(), DeletedFilter.DELETED_VALUE);
			try {
				writer.addMutation(m);
			} catch (MutationsRejectedException e) {
				throw new MongoException(e.getLocalizedMessage());
			}

			writeResult.setN(1);
			return writeResult;
		}

		int affected = 0;
		// look in the index for items matching the query
		DBCursor cur = find(query, null);
		for(DBObject doc : cur) {
			rowid = ((ObjectId)doc.get("_id")).toString();
			// remove each from index
			try {
				indexBSON(doc, "", rowid, null, true);
				
				// then main table
				Mutation m = new Mutation(rowid);
				m.put("BSON", "", Long.MAX_VALUE - System.currentTimeMillis(), DeletedFilter.DELETED_VALUE);
				writer.addMutation(m);

			} catch (Exception e) {
				throw new MongoException(e.getLocalizedMessage());
			}
			
			affected++;
		}

		flush(); // call ?

		writeResult.setN(affected);
		return writeResult;
	}


	public DBCursor find() {
		return find(null, null, 0, 0);
	}

	public DBCursor find(String ref) {
		return find(new BasicDBObject((Map)JSON.parse(ref)));
	}
	
	public DBCursor find(DBObject ref) {
		return find(ref, null, 0, 0);
	}

	public DBCursor find(DBObject ref, DBObject keys) {
		return find(ref, keys, 0, 0);
	}

	public DBCursor find(String ref, String keys) {
		return find(new BasicDBObject((Map)JSON.parse(ref)), new BasicDBObject((Map)JSON.parse(keys)));
	}
	
	public DBCursor find(DBObject query, DBObject fields, int numToSkip, int batchSize, int options) throws MongoException {

		return find(query, fields, numToSkip, batchSize);
	}


	public DBCursor find(DBObject query, DBObject select, int numToSkip, int batchSize) {

		// provides consistency from this client's point of view
		if(dirty)
			flush();

		try {
			return new DBCursor(this, query, select, numToSkip);
		} catch (TableNotFoundException e) {
			throw new MongoException(e.getLocalizedMessage());
		}
	}


	public DBObject findOne() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	public DBObject findOne(DBObject o) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	public DBObject findOne(Object obj) throws MongoException {
		
		return findOne(obj, null);
	}

	public DBObject findOne(String o, String fields) {
		return findOne(new BasicDBObject((Map)JSON.parse(o)), new BasicDBObject((Map)JSON.parse(fields)));
	}
	
	public DBObject findOne(DBObject o, DBObject fields) {
		// TODO Auto-generated method stub
		return null;
	}

	public DBObject findOne(DBObject o, DBObject fields, ReadPreference readPref) {
		// TODO Auto-generated method stub
		return null;
	}

	public DBObject get(Object id, String select) {
		return get(id, new BasicDBObject((Map)JSON.parse(select)));
	}
	
	public DBObject get(Object id, BSONObject select) {
		
		String rowid = null;
		if(id instanceof String) {
			rowid = (String)id;
		}
		else if(id instanceof ObjectId) {
			rowid = ((ObjectId)id).toString();
		}
		
		if(dirty)
			flush();

		simpleGetter.setRange(new Range(rowid));

		//if(select != null)
		//	SelectIterator.setSelectOnScanner(simpleGetter, select);
		
		DBObject result = null;
		Iterator<Entry<Key, Value>> iter = simpleGetter.iterator();
		if(iter.hasNext()) {
			Entry<Key, Value> pair = iter.next();
			result = BSONHelper.objectForEntry(pair);
		}
		
		//SelectIterator.removeSelectOnScanner(simpleGetter);
		return result;
	}

	public DBObject findOne(Object obj, DBObject select) {

		// findOne({"_id:x"}) is the same as get(x)

		if(obj instanceof String || obj instanceof ObjectId) {
			return get(obj, (DBObject)null);
		}
		else if(obj instanceof DBObject) {
			DBObject query = (DBObject)obj;
			
			if(query.keySet().size() == 1 && query.get("_id") != null) {
				return get(query.get("_id"), (DBObject)null);
			}
			
			DBCursor cursor = find(query, select);
			if(!cursor.hasNext())
				return null;
			
			return cursor.next();
		}

		return null;
	}


	public DBObject findAndModify(DBObject query, DBObject fields,
			DBObject sort, boolean remove, DBObject update, boolean returnNew,
			boolean upsert) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBObject findAndModify(DBObject query, DBObject sort, DBObject update) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBObject findAndModify(DBObject query, DBObject update) {
		// TODO Auto-generated method stub
		return null;
	}


	public DBObject findAndRemove(DBObject query) {
		// TODO Auto-generated method stub
		return null;
	}

	public void createIndex(DBObject obj) throws MongoException {
		createIndex(obj, false);
	}
	
	public void createIndex(DBObject obj, boolean block) throws MongoException {
		
		if(dirty)
			flush();

		Iterator<String> kiter = obj.keySet().iterator();

		while(kiter.hasNext()) {
			String key = (String) kiter.next(); 

			// check that index doesn't already exist
			if(metadata.getIndexedKeys(tableName).contains(key)) 
				return;
			
			// TODO: should also disallow building indexes on _id

			try {
				getWritersReaders();
			

			conn.tableOperations().create(tableNameForIndex(key));

			// TODO: communicate new index key to other clients via zookeeper
			metadata.setIndexKey(tableName, key);
			
			} catch (Exception e1) {
				throw new MongoException(e1.getLocalizedMessage());
			} 
			
			indexedKeys.add(key);

			// start indexing existing keys via MR
			String[] args = {
					"command",
					conn.getInstance().getInstanceName().toString(),
					conn.getInstance().getZooKeepers(),
					db.jaccson.username,
					db.jaccson.password,
					tableName,
					key,
					block ? "block" : "noblock"
			};

			try {
				ToolRunner.run(new BuildIndexMR(), args);
			} catch (Exception e) {
				throw new MongoException(e.getLocalizedMessage());
			}
		}
	}


	public void createIndex(DBObject keys, DBObject options)
			throws MongoException {
		createIndex(keys);
	}


	public void createIndex(DBObject keys, DBObject options, DBEncoder encoder)
			throws MongoException {
		createIndex(keys);
	}


	public void ensureIndex(String obj) {
		DBObject keys = new BasicDBObject((Map)JSON.parse(obj));
		ensureIndex(keys);
	}


	public void ensureIndex(DBObject keys) throws MongoException {
		createIndex(keys);
	}


	public void ensureIndex(DBObject keys, String name) throws MongoException {
		createIndex(keys);
	}


	public void ensureIndex(DBObject keys, String name, boolean unique)
			throws MongoException {
		createIndex(keys);
	}


	public void ensureIndex(DBObject keys, DBObject optionsIN)
			throws MongoException {
		createIndex(keys);
	}


	public void resetIndexCache() {
		
	}


	public void setHintFields(List<DBObject> lst) {
		
	}


	public Object apply(DBObject o) {
		// TODO Auto-generated method stub
		return null;
	}


	public Object apply(DBObject jo, boolean ensureID) {
		// TODO Auto-generated method stub
		return null;
	}


	public WriteResult save(DBObject jo) {
		// TODO Auto-generated method stub
		return null;
	}


	public WriteResult save(DBObject jo, WriteConcern concern)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	public void dropIndexes() throws MongoException {
		// TODO Auto-generated method stub

	}


	public void dropIndexes(String name) throws MongoException {
		// TODO Auto-generated method stub

	}

	
	public void drop() throws MongoException {

		if(db.jaccson.conn.tableOperations().exists(tableName)) {

			//JSONObject dropIndexes = new JSONObject();
			DBObject dropIndexes = new BasicDBObject();
			for(String indexedKey : indexedKeys) {

				dropIndexes.put(indexedKey, 1);
			}

			dropIndex(dropIndexes);
			
			try {
				db.removeCollection(name);
				db.jaccson.conn.tableOperations().delete(tableName);
			} catch (TableNotFoundException e) {
				e.printStackTrace();
			} catch (AccumuloException e) {
				e.printStackTrace();
			} catch (AccumuloSecurityException e) {
				e.printStackTrace();
			}
		}
	}


	public long count() throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public long count(DBObject query) throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public long getCount() throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public long getCount(DBObject query) throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public long getCount(DBObject query, DBObject fields) throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public long getCount(DBObject query, DBObject fields, long limit, long skip)
			throws MongoException {
		// TODO Auto-generated method stub
		return 0;
	}


	public DBCollection rename(String newName)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	public DBCollection rename(String newName, boolean dropTarget)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	@SuppressWarnings("rawtypes")
	public List distinct(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	@SuppressWarnings("rawtypes")
	public List distinct(String key, DBObject query) {
		// TODO Auto-generated method stub
		return null;
	}


	public MapReduceOutput mapReduce(String map, String reduce,
			String outputTarget, DBObject query) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	public MapReduceOutput mapReduce(String map, String reduce,
			String outputTarget, OutputType outputType, DBObject query)
					throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	public MapReduceOutput mapReduce(MapReduceCommand command)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	public MapReduceOutput mapReduce(DBObject command) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}


	public List<DBObject> getIndexInfo() {
		// TODO Auto-generated method stub
		return null;
	}


	public void dropIndex(DBObject keys) throws MongoException {
		// TODO Auto-generated method stub

	}


	public void dropIndex(String name) throws MongoException {
		// TODO Auto-generated method stub

	}


	public CommandResult getStats() {
		// TODO Auto-generated method stub
		return null;
	}


	public boolean isCapped() {

		return false;
	}


	public DBCollection getCollection(String n) {
		// TODO Auto-generated method stub
		return null;
	}


	public String getName() {

		return name;
	}


	public String getFullName() {
		// TODO Auto-generated method stub
		return null;
	}


	public DB getDB() {

		return db;
	}


	@SuppressWarnings("rawtypes")
	public void setObjectClass(Class c) {
		// TODO Auto-generated method stub

	}


	@SuppressWarnings("rawtypes")
	public Class getObjectClass() {
		// TODO Auto-generated method stub
		return null;
	}


	@SuppressWarnings("rawtypes")
	public void setInternalClass(String path, Class c) {
		// TODO Auto-generated method stub

	}


	public void setWriteConcern(WriteConcern concern) {
		// TODO Auto-generated method stub

	}


	public WriteConcern getWriteConcern() {
		// TODO Auto-generated method stub
		return null;
	}


	public void setReadPreference(ReadPreference preference) {
		// TODO Auto-generated method stub

	}


	public ReadPreference getReadPreference() {
		// TODO Auto-generated method stub
		return null;
	}


	public void slaveOk() {
		// TODO Auto-generated method stub

	}


	public void addOption(int option) {
		// TODO Auto-generated method stub

	}


	public void setOptions(int options) {
		// TODO Auto-generated method stub

	}


	public void resetOptions() {
		// TODO Auto-generated method stub

	}


	public int getOptions() {
		// TODO Auto-generated method stub
		return 0;
	}


	public void setDBDecoderFactory(DBDecoderFactory fact) {
		// TODO Auto-generated method stub

	}


	public DBDecoderFactory getDBDecoderFactory() {
		// TODO Auto-generated method stub
		return null;
	}


	public void setDBEncoderFactory(DBEncoderFactory fact) {
		// TODO Auto-generated method stub

	}


	public DBEncoderFactory getDBEncoderFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	public void close() {

		for(BatchWriter w : indexWriters.values()) {
			try {
				w.close();
			} catch (MutationsRejectedException e) {
				e.printStackTrace();
			}
		}

		try {
			writer.close();
		} catch (MutationsRejectedException e) {
			e.printStackTrace();
		}	
	}
	
	public void flush() {

		for(BatchWriter w : indexWriters.values()) {
			try {
				w.flush();
			} catch (MutationsRejectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			writer.flush();
		} catch (MutationsRejectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		dirty = false;
	}

	public boolean isIndexed(String name2) {
		// TODO Auto-generated method stub
		return false;
	}
	
	BatchScanner batchScannerForKey(String field) throws TableNotFoundException {
		return conn.createBatchScanner(tableNameForIndex(field), auths, 10);
	}
	
	BatchScanner batchScanner() throws TableNotFoundException {

		return conn.createBatchScanner(tableName, auths, 10);
	}

	Scanner indexScannerForKey(String key) {
		try {
			return conn.createScanner(tableNameForIndex(key), auths);
		} catch (TableNotFoundException e) {
			return null;
		}
	}


	public void compact() {
		// TODO Auto-generated method stub
		
	}
}
