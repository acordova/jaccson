/**
 * AccJSONTable
 * 
 * This class provides most of the client API functionality
 * 
 * The other class is AccJSONCursor, which provides the capability to read out documents
 * 
 * this class should be made thread-safe
 * 
 */

package com.jaccson;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.server.JaccsonUpdater;
import com.jaccson.server.SelectIterator;
import com.jaccson.server.JaccsonUpdater.operator;

public class JaccsonTable {

	protected String tableName;
	private BatchWriter writer = null;
	protected Authorizations auths; 
	protected Connector conn;
	private Scanner simpleGetter;
	private Set<String> indexedKeys;
	private TableMetadata metadata;
	private String username;
	private String password;
	private boolean dirty;

	private HashMap<String,BatchWriter> indexWriters;

	private static final Value BLANK_VALUE = new Value("".getBytes());
	
	private void createTable(String tableName) throws TableNotFoundException {
		try {
			conn.tableOperations().create(tableName);

			// remove default versioning iterator
			conn.tableOperations().removeIterator(tableName, "vers", EnumSet.allOf(IteratorUtil.IteratorScope.class));
			
			
			// apply update iterator
			IteratorSetting upiterset = new IteratorSetting(IterStack.UPDATER_ITERATOR_PRI, "jaccsonUpdater", "com.jaccson.server.JaccsonUpdater");

			ArrayList<IteratorSetting.Column> cols = new ArrayList<IteratorSetting.Column>();
			cols.add(new IteratorSetting.Column("JSON"));
			Combiner.setColumns(upiterset, cols);
			conn.tableOperations().attachIterator(tableName, upiterset);
			
			
			// apply deleted filter
			IteratorSetting deliterset = new IteratorSetting(IterStack.DELETED_ITERATOR_PRI, "jaccsonDeleter", "com.jaccson.server.DeletedFilter");
			conn.tableOperations().attachIterator(tableName, deliterset);
			

			// TODO: need to throw any of these?
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		}
	}
	
	private void getWritersReaders() throws TableNotFoundException {

		if(writer != null)
			return;

		if(!conn.tableOperations().exists(tableName)) {
			createTable(tableName);
		}

		writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 10);
		indexWriters = new HashMap<String,BatchWriter>();
		simpleGetter = conn.createScanner(tableName, auths);
		simpleGetter.setBatchSize(1);
	}

	public JaccsonTable(String table, Connector conn, Authorizations auths, String username, String password) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

		this.tableName = table;

		metadata = new TableMetadata(conn, auths);

		indexedKeys = metadata.getIndexedKeys(tableName); 
		dirty = false;

		this.auths = auths;
		this.conn = conn;
		this.username = username;
		this.password = password;
	}

	private BatchWriter writerForIndexKey(String key) throws TableNotFoundException {

		if(!indexWriters.containsKey(key)) {
			indexWriters.put(key, conn.createBatchWriter(tableNameForIndex(key), 1000000L, 1000L, 10));
		}

		return indexWriters.get(key);
	}

	private void indexJSON(Object obj, String prefix, String rowid) throws MutationsRejectedException, JSONException, TableNotFoundException {
		indexJSON( obj,  prefix,  rowid, null, false);
	}

	private void indexJSON(Object obj, String prefix, String rowid, ColumnVisibility cv) throws MutationsRejectedException, JSONException, TableNotFoundException {
		indexJSON(obj, prefix, rowid, cv, false);
	}

	private void indexJSON(Object obj, String prefix, String rowid, ColumnVisibility cv, boolean delete) throws JSONException, MutationsRejectedException, TableNotFoundException {

		if(obj instanceof JSONObject) {

			JSONObject jobj = (JSONObject) obj;

			for(String name : JSONObject.getNames(jobj)) {
				Object o = jobj.get(name);

				indexJSON(o, prefix + name + ".", rowid, cv, delete);
			}
		}

		else if (obj instanceof JSONArray) {

			JSONArray oarr = (JSONArray)obj;

			// index under the same name?
			for(int i=0; i < oarr.length(); i++) {
				indexJSON(oarr.get(i), prefix, rowid, cv, delete);
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


	public String insert(String json) throws MutationsRejectedException, TableNotFoundException, JSONException {
		return insert(new JSONObject(json));
	}

	public String insert(JSONObject obj) throws MutationsRejectedException, TableNotFoundException, JSONException {

		// extract col vis
		String cvstring = null;
		try {
			cvstring = obj.getString("$security");
		}
		catch (JSONException je) {
			
		}
		
		ColumnVisibility cv = null;
		if(cvstring != null)
			cv = new ColumnVisibility(cvstring);

		getWritersReaders();

		String rowid = null;
		if(obj.has("_id")) {
			rowid = obj.getString("_id");
			obj.remove("_id");
		}
		else {
			// generate ID
			rowid = UUID.randomUUID().toString();
		}

		Mutation m = new Mutation(rowid);
		if(cv == null)
			m.put("JSON", "", Long.MAX_VALUE - System.currentTimeMillis(), new Value(obj.toString().getBytes()));
		else
			m.put("JSON", "", cv, Long.MAX_VALUE - System.currentTimeMillis(), new Value(obj.toString().getBytes()));

		writer.addMutation(m);

		// might want to flush writer here, in case index is written before main mutation

		// if no indexed keys, skip
		if(indexedKeys.size() > 0)
			indexJSON(obj, "", rowid, cv);

		dirty = true;

		// TODO: update count

		return rowid;	
	}

	public int update(String query, String mods) throws MutationsRejectedException, JSONException, TableNotFoundException {
		return update(new JSONObject(query), new JSONObject(mods));
	}

	public int update(JSONObject query, JSONObject mods) throws JSONException, TableNotFoundException, MutationsRejectedException {

		return update(query, mods, false, false);
	}

	public int update(JSONObject query, JSONObject mods, boolean upsert) throws JSONException, TableNotFoundException, MutationsRejectedException {
		return update(query, mods, upsert, false);
	}

	// TODO: if updating _id, need to retrieve, delete, then reinsert
	// with the new _id
	//
	// TODO: remove multi? default to multi=true?
	public int update(JSONObject query, JSONObject modsj, boolean upsert, boolean multi) throws JSONException, TableNotFoundException, MutationsRejectedException {

		getWritersReaders();

		JaccsonCursor cur = find(query, null);
		boolean found = false;
		for(JSONObject o : cur) {
			found = true;

			String objid = o.getString("_id");
			Mutation m = new Mutation(objid);
			@SuppressWarnings("rawtypes")
			// break update into several objects
			Iterator keyIter = modsj.keys();
			while(keyIter.hasNext()) {
				JSONObject mod = new JSONObject();
				String command = (String) keyIter.next();

				// verify command is legal
				// throws IllegalArgumentException
				JaccsonUpdater.operator.valueOf(command);

				mod.put(command, modsj.getJSONObject(command));
				m.put("JSON", "", Long.MAX_VALUE - System.currentTimeMillis(), new Value(mod.toString().getBytes()));
				writer.addMutation(m);

				// update indexes
				indexUpdate(mod, objid);
			}

			if(!multi)
				break;
		}

		if(!found && upsert) {
			@SuppressWarnings("rawtypes")
			Iterator kiter = modsj.keys();

			while(kiter.hasNext()) {
				String key = (String) kiter.next();
				query.put(key, modsj.get(key));
			}

			insert(query);
		}

		dirty = true;

		return 0;
	}

	private void indexUpdate(JSONObject mod, String rowID) throws JSONException, MutationsRejectedException, TableNotFoundException {

		String command = (String) mod.keys().next();
		JSONObject update = (JSONObject) mod.get(command);
		String prefix = (String)update.keys().next();
		Object obj = update.get(prefix);

		operator op = operator.valueOf(command);

		switch(op) {
		case $inc:
		{
			// retrieve, remove, insert?
		}
		case $set:
		{
			indexJSON(obj, prefix, rowID);
			break;
		}
		case $unset:
		{
			indexJSON(obj, prefix, rowID, null, true);
			break;
		}
		case $push:
		{
			indexJSON(obj, prefix, rowID);
			break;
		}
		case $pushAll:
		{
			indexJSON(obj, prefix, rowID);
			break;
		}
		case $addToSet:
		{
			indexJSON(obj, prefix, rowID);
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
			indexJSON(obj, prefix, rowID, null, true);
			break;
		}
		case $pullAll:
		{
			indexJSON(obj, prefix, rowID, null, true);
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

	public boolean remove(String query) throws TableNotFoundException, JSONException, MutationsRejectedException {
		return remove(new JSONObject(query));
	}

	public boolean remove(JSONObject query) throws TableNotFoundException, JSONException, MutationsRejectedException {

		getWritersReaders();

		String rowid = query.getString("_id");

		// just delete this one object
		if(rowid != null) { // TODO: look at any other clauses? warn user that they're ignored?
			Mutation m = new Mutation(rowid);
			m.put("JSON", "", Long.MAX_VALUE - System.currentTimeMillis(), "{$delete:1}");
			writer.addMutation(m);

			return true;
		}

		boolean found = false;
		// look in the index for items matching the query
		JaccsonCursor cur = find(query, null);
		for(JSONObject doc : cur) {
			rowid = doc.getString("_id");
			// remove each from index
			indexJSON(doc, "", rowid, null, true);

			// then main table
			Mutation m = new Mutation(rowid);
			m.put("JSON", "", Long.MAX_VALUE - System.currentTimeMillis(), "{$delete:1}");
			writer.addMutation(m);

			found = true;
		}

		flush(); // call ?

		return found;
	}

	public JaccsonCursor find(String query) throws TableNotFoundException, JSONException {
		return find(query, null);
	}
	
	public JaccsonCursor find(String query, String select) throws TableNotFoundException, JSONException {
		if(select == null || select.equals(""))
			return find(new JSONObject(query), null);
		else
			return find(new JSONObject(query), new JSONObject(select));
	}

	public JaccsonCursor find(JSONObject query) throws TableNotFoundException, JSONException {
		return find(query, null);
	}
	
	public JaccsonCursor find(JSONObject query, JSONObject select) throws TableNotFoundException, JSONException {

		// provides consistency from this client's point of view
		if(dirty)
			flush();

		return new JaccsonCursor(this, query, select);
	}

	public JSONObject findOne(String query) throws JSONException, TableNotFoundException {
		return findOne(query, null);
	}
	
	public JSONObject findOne(String query, String select) throws JSONException, TableNotFoundException {
		if(select == null) {
			return findOne(new JSONObject(query), null);
		}
		else {
			return findOne(new JSONObject(query), new JSONObject(select));
		}
	}

	public JSONObject findOne(JSONObject query) throws JSONException, TableNotFoundException {
		return findOne(query, null);
	}
	
	public JSONObject findOne(JSONObject query, JSONObject select) throws JSONException, TableNotFoundException {

		// findOne({"_id:x"}) is the same as get(x)
		
		if(query != null && query.length() == 1 && query.get("_id") != null) {
			return get(query.getString("_id"));
		}
		
		JaccsonCursor cursor = find(query, select);
		if(!cursor.hasNext())
			return null;
		
		return cursor.next();
	}

	public JSONObject get(String rowid) throws JSONException {

		return get(rowid, (JSONObject)null);
	}
	
	public JSONObject get(String rowid, String select) throws JSONException {
		
		return get(rowid, new JSONObject(select));
	}
	
	public JSONObject get(String rowid, JSONObject select) {
		
		if(dirty)
			flush();

		simpleGetter.setRange(new Range(rowid));

		if(select != null)
			SelectIterator.setSelectOnScanner(simpleGetter, select);
		
		JSONObject result = null;
		Iterator<Entry<Key, Value>> iter = simpleGetter.iterator();
		if(iter.hasNext()) {
			Entry<Key, Value> pair = iter.next();
			result = JSONHelper.objectForEntry(pair);
		}
		
		SelectIterator.removeSelectOnScanner(simpleGetter);
		return result;
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

	public void ensureIndex(String json) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, JSONException {
		ensureIndex(new JSONObject(json));
	}
	
	/**
	 * note - there are no 'composite indexes', rather all indexed fields are always used 
	 * so there is no difference between indexing two fields separately or together
	 * 
	 * @param key
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 * @throws TableNotFoundException
	 */
	public void ensureIndex(JSONObject obj) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		ensureIndex(obj, false);
	}

	public void ensureIndex(JSONObject obj, boolean block) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {

		if(dirty)
			flush();

		@SuppressWarnings("rawtypes")
		Iterator kiter = obj.keys();

		while(kiter.hasNext()) {
			String key = (String) kiter.next(); 

			// check that index doesn't already exist
			if(metadata.getIndexedKeys(tableName).contains(key)) 
				return;
			
			// TODO: should also disallow building indexes on _id

			getWritersReaders();

			conn.tableOperations().create(tableNameForIndex(key));

			// TODO: communicate new index key to other clients via zookeeper
			metadata.setIndexKey(tableName, key);
			indexedKeys.add(key);

			// start indexing existing keys via MR
			String[] args = {
					"command",
					conn.getInstance().getInstanceName().toString(),
					conn.getInstance().getZooKeepers(),
					username,
					password,
					tableName,
					key,
					block ? "block" : "noblock"
			};

			try {
				ToolRunner.run(new BuildIndexMR(), args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void dropIndex(JSONObject obj) throws AccumuloException, AccumuloSecurityException {

		@SuppressWarnings("rawtypes")
		Iterator kiter = obj.keys();

		while(kiter.hasNext()) {

			String key = (String) kiter.next();

			metadata.removeIndexKey(tableName, key);

			// drop index table
			try {
				conn.tableOperations().delete(tableName + "_" + key.replace('.', '_'));
			} catch (TableNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public void compact() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
		compact(false, false);
	}
	
	public void compact(boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
		conn.tableOperations().compact(tableName, null, null, true, wait);
	}

	public void drop() throws AccumuloException, AccumuloSecurityException {

		JSONObject dropIndexes = new JSONObject();
		for(String indexedKey : indexedKeys) {
			try {
				dropIndexes.put(indexedKey, 1);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		dropIndex(dropIndexes);

		try {
			conn.tableOperations().delete(tableName);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}

	protected BatchScanner batchScanner() throws TableNotFoundException {

		return conn.createBatchScanner(tableName, auths, 10);
	}

	protected Scanner indexScannerForKey(String key) {
		try {
			return conn.createScanner(tableNameForIndex(key), auths);
		} catch (TableNotFoundException e) {
			return null;
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

	public BatchScanner batchScannerForKey(String field) throws TableNotFoundException {
		return conn.createBatchScanner(tableNameForIndex(field), auths, 10);
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, 
	TableNotFoundException, TableExistsException, JSONException {

		JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

		JaccsonTable table = conn.getTable("indexEmptyTestTable");
		table.ensureIndex(new JSONObject("{field:1}"));

		table.insert(new JSONObject("{field:'aaa', amount:2}"));
		table.insert(new JSONObject("{field:'bbb', amount:2}"));
		table.insert(new JSONObject("{field:'ccc', amount:2}"));
		table.insert(new JSONObject("{field:'ddd', amount:2}"));
		table.insert(new JSONObject("{field:'eee', amount:2}"));
		table.insert(new JSONObject("{field:'fff', amount:2}"));
		table.insert(new JSONObject("{field:'ggg', amount:2}"));
	}

	public boolean isIndexed(String name) {

		return indexedKeys.contains(name);
	}
}
