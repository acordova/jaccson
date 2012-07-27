package com.jaccson.mongo;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

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
import org.apache.accumulo.core.security.Authorizations;

public class TableMetadata {
	
	private final String JACCSON_MD_TABLE = "jaccson_metadata";
	
	private BatchWriter writer;
	private Scanner scanner;

	public TableMetadata(Connector conn, Authorizations auths) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
			
		try {
			writer = conn.createBatchWriter(JACCSON_MD_TABLE, 1000000L, 1000L, 10);
			scanner = conn.createScanner(JACCSON_MD_TABLE, auths);	
			
		} catch (TableNotFoundException e) {
			try {
				conn.tableOperations().create(JACCSON_MD_TABLE);
			} catch (TableExistsException e1) {
				e1.printStackTrace();
			}
			
			writer = conn.createBatchWriter(JACCSON_MD_TABLE, 1000000L, 1000L, 10);
			scanner = conn.createScanner(JACCSON_MD_TABLE, auths);	
		}
	}
	
	public void setIndexKey(String table, String key) throws MutationsRejectedException {
		
		Mutation m = new Mutation(table);
		m.put("indexed_keys", key, new Value("".getBytes()));
		writer.addMutation(m);
		writer.flush();
	}
	
	public Set<String> getIndexedKeys(String table) {
		
		scanner.setRange(new Range(table));
		
		HashSet<String> keys = new HashSet<String>();
		for(Entry<Key,Value> e : scanner) {
			keys.add(e.getKey().getColumnQualifier().toString());
		}
		
		return keys;
	}

	public void removeIndexKey(String tableName, String key) throws MutationsRejectedException {
		
		Mutation m = new Mutation(tableName);
		m.putDelete("indexed_keys", key);
		writer.addMutation(m);
		writer.flush();
	}
}
