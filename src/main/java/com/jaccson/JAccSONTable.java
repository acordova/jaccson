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
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JAccSONTable {

	protected String tableName;
	private BatchWriter writer;
	protected Authorizations auths; 
	protected Connector conn;
	private Scanner simpleGetter;
	private Set<String> indexedKeys;
	private TableMetadata metadata;

	private HashMap<String,BatchWriter> indexWriters;

	private static final Value BLANK_VALUE = new Value("".getBytes());

	public JAccSONTable(String table, Connector conn, Authorizations auths) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

		this.tableName = table;

		writer = conn.createBatchWriter(table, 1000000L, 1000L, 10);

		indexWriters = new HashMap<String,BatchWriter>();
		simpleGetter = conn.createScanner(table, auths);
		simpleGetter.setBatchSize(1);

		metadata = new TableMetadata(conn, auths);

		indexedKeys = metadata.getIndexedKeys(tableName); 

		this.auths = auths;
		this.conn = conn;
	}

	private BatchWriter writerForIndexKey(String key) throws TableNotFoundException {

		if(!indexWriters.containsKey(key)) {
			indexWriters.put(key, conn.createBatchWriter(tableName + "_" + key.replace(',', '_'), 1000000L, 1000L, 10));
		}

		return indexWriters.get(key);
	}

	private void indexJSON(Object obj, String prefix, String rowid) throws JSONException, MutationsRejectedException, TableNotFoundException {

		if(obj instanceof JSONObject) {

			JSONObject jobj = (JSONObject) obj;

			for(String name : JSONObject.getNames(jobj)) {
				Object o = jobj.get(name);

				indexJSON(o, prefix + name + ".", rowid);
			}
		}

		else if (obj instanceof JSONArray) {

			JSONArray oarr = (JSONArray)obj;

			// index under the same name?
			for(int i=0; i < oarr.length(); i++) {
				indexJSON(oarr.get(i), prefix, rowid);
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
					m.put(tableName + "_" + prefix, rowid, BLANK_VALUE);

					indexWriter.addMutation(m);
				}
			}
			else {
				byte[] bytes = IndexHelper.indexValueForObject(obj);

				Mutation m = new Mutation(new Text(bytes));
				m.put(tableName + "_" + prefix, rowid, BLANK_VALUE);

				indexWriter.addMutation(m);
			}
		}
	}


	public String insert(String json) throws JSONException, MutationsRejectedException, TableNotFoundException {

		JSONObject obj = new JSONObject(json);

		String rowid = null;
		if(obj.has("_id")) {
			rowid = obj.getString("_id");
		}
		else {
			// generate ID
			rowid = UUID.randomUUID().toString();
		}

		Mutation m = new Mutation(rowid);
		m.put("JSON", "", new Value(json.getBytes()));
		writer.addMutation(m);

		// might want to flush writer here, in case index is written before main mutation

		// if no indexed keys, skip
		if(indexedKeys.size() > 0)
			indexJSON(obj, "", rowid);

		// TODO: update count

		return rowid;	
	}

	public int update(String query, String mods) throws JSONException, TableNotFoundException, MutationsRejectedException {

		return update(query, mods, false, false);
	}

	public int update(String query, String mods, boolean upsert) throws JSONException, TableNotFoundException, MutationsRejectedException {
		return update(query, mods, upsert, false);
	}
	
	// TODO: remove multi? default to multi=true?
	public int update(String query, String mods, boolean upsert, boolean multi) throws JSONException, TableNotFoundException, MutationsRejectedException {
		// parse mods
		JSONObject modsj = new JSONObject(mods);

		JAccSONCursor cur = find(query, "");
		boolean found = false;
		for(JSONObject o : cur) {
			found = true;
			
			Mutation m = new Mutation(o.getString("_id"));
			@SuppressWarnings("rawtypes")
			Iterator keyIter = modsj.keys();
			while(keyIter.hasNext()) {
				JSONObject mod = modsj.getJSONObject((String) keyIter.next());
				m.put("JSON", "", new Value(mod.toString().getBytes()));
			}
			
			if(!multi)
				break;
		}
		
		if(!found && upsert) {
			// TODO: check that there are no operators
			insert(mods);
		}
		
		return 0;
	}

	public boolean remove(String query) {

		// look in the index for items matching the query
		
		// remove each from index
		// then main table
		return false;
	}

	public JAccSONCursor find(String query, String select) throws TableNotFoundException, JSONException {

		return new JAccSONCursor(this, query, select);
	}

	public JSONObject findOne(String query, String select) throws TableNotFoundException, JSONException {

		JAccSONCursor cursor = new JAccSONCursor(this, query, select);

		if(!cursor.hasNext())
			return null;

		return cursor.next();
	}

	public JSONObject get(String rowid) throws JSONException {

		simpleGetter.setRange(new Range(rowid));

		Iterator<Entry<Key, Value>> iter = simpleGetter.iterator();
		if(!iter.hasNext())
			return null;
		return new JSONObject(new String(iter.next().getValue().get()));
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

	public void ensureIndex(String key) throws AccumuloException, AccumuloSecurityException, TableExistsException {

		// check that index doesn't already exist
		if(metadata.getIndexedKeys(tableName).contains(key)) 
			return;


		// TODO: communicate new index key to other clients via zookeeper
		metadata.setIndexKey(tableName, key);
		indexedKeys.add(key);

		conn.tableOperations().create(tableName + "_" + key.replace('.', '_'));

		// start indexing existing keys via MR
		String[] args = {
				"command",
				conn.getInstance().getInstanceName(),
				conn.getInstance().getZooKeepers(),
				"username",
				"password",
				tableName,
				key
		};

		try {
			ToolRunner.run(new BuildIndexMR(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void dropIndex(String key) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		metadata.removeIndexKey(tableName, key);

		// drop index table
		conn.tableOperations().delete(tableName + "_" + key.replace('.', '_'));
	}

	public void compact() {
		// TODO: implement

	}

	public void drop() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

		for(String indexedKey : indexedKeys) {
			dropIndex(indexedKey);
		}

		conn.tableOperations().delete(tableName);
	}

	protected BatchScanner batchScanner() throws TableNotFoundException {

		return conn.createBatchScanner(tableName, auths, 10);
	}

	protected Scanner indexScannerForKey(String key) throws TableNotFoundException {
		return conn.createScanner(tableName + "_" + key.replace('.', '_'), auths);
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
	}

	public BatchScanner batchScannerForKey(String field) throws TableNotFoundException {
		return conn.createBatchScanner(tableName + "_" + field.replace('.', '_'), auths, 10);
	}
}
