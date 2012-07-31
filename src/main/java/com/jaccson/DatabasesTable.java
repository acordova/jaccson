package com.jaccson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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
import org.apache.hadoop.io.Text;

public class DatabasesTable {
	
	private Scanner dbScanner;
	private BatchWriter dbWriter;
	
	private static final Text INFO_TEXT = new Text("INFO");
	private static final Text COLLECTIONS_TEXT = new Text("COLLECTIONS");
	
	public DatabasesTable(Jaccson jaccson) {
		
		Connector conn = jaccson.conn;
		
		if(!conn.tableOperations().exists("jaccson_databases")) {
			try {
				conn.tableOperations().create("jaccson_databases");
			} catch (TableExistsException e) {
				e.printStackTrace();
			} catch (AccumuloException e) {
				e.printStackTrace();
			} catch (AccumuloSecurityException e) {
				e.printStackTrace();
			}
		}
		
		try {
			dbScanner = conn.createScanner("jaccson_databases", jaccson.auths);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			dbWriter = conn.createBatchWriter("jaccson_databases", 1000000L, 1000L, 2);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void addDatabase(String db) throws IOException, MutationsRejectedException {
		Mutation m = new Mutation(db);
		
		m.put("INFO", "creation", new Value(Value.longToBytes(System.currentTimeMillis())));
		dbWriter.addMutation(m);
		dbWriter.flush();
	}
	
	public void addCollection(String db, String coll) throws MutationsRejectedException {
		
		Mutation m = new Mutation(db);
		m.put("COLLECTIONS", coll, new Value("".getBytes()));
		dbWriter.addMutation(m);
		dbWriter.flush();
		
	}

	public boolean existsCollection(String db, String collectionName) {
		Key k = new Key(db, "COLLECTION", collectionName);
		dbScanner.clearColumns();
		dbScanner.setRange(new Range(k, k));
		
		return dbScanner.iterator().hasNext();
	}
	
	public void removeCollection(String db, String coll) throws MutationsRejectedException {
		Mutation m = new Mutation(new Text(db));
		m.putDelete("COLLECTIONS", coll);
		
		dbWriter.addMutation(m);
		dbWriter.flush();
	}
	
	public void removeDatabase(String db) throws MutationsRejectedException {
		
		Mutation m = new Mutation(new Text(db));
		m.putDelete("INFO","creation");
		dbWriter.addMutation(m);
		dbWriter.flush();
	}
	
	public ArrayList<String> listCollections(String db) {
		
		ArrayList<String> collections = new ArrayList<String>();

		dbScanner.setRange(new Range(db));
		dbScanner.clearColumns();
		dbScanner.fetchColumnFamily(COLLECTIONS_TEXT);
		
		for(Entry<Key, Value> e : dbScanner) {
			String collname = e.getKey().getColumnQualifier().toString();
			
			collections.add(collname);
		}
		
		return collections;
	}

	public boolean existsDB(String db) {
		dbScanner.setRange(new Range(db));
		dbScanner.clearColumns();
		dbScanner.fetchColumnFamily(INFO_TEXT);
		
		return dbScanner.iterator().hasNext();
	}

	public List<String> listDBs() {
		ArrayList<String> dbs = new ArrayList<String>();
		
		dbScanner.clearColumns();
		dbScanner.fetchColumnFamily(INFO_TEXT);
		for(Entry<Key, Value> e : dbScanner) {
			dbs.add(e.getKey().getRow().toString());
		}
		
		return dbs;
	}
}
