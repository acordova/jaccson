package com.jaccson.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.DBTCPConnector;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;


public class Jaccson  {

	Instance inst;
	Connector conn;
	Authorizations auths;
	String username;
	String password;
	private Scanner dbScanner;
	private BatchWriter dbWriter;
	private HashMap<String,DB> opendbs;
	
	public Jaccson(String zkServers, String instance, String user, String pass, String auths) throws AccumuloException, AccumuloSecurityException {
		
		username = user;
		password = pass;
		
		inst = new ZooKeeperInstance(instance, zkServers);
		conn = inst.getConnector(user, pass);
		
		if(auths == null || auths.equals(""))
			this.auths = new Authorizations();
		else
			this.auths = new Authorizations(auths.split(","));
		
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
			dbScanner = conn.createScanner("jaccson_databases", this.auths);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			dbWriter = conn.createBatchWriter("jaccson_databases", 1000000L, 1000L, 2);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		
		opendbs = new HashMap<String,DB>();
	}
	
	
	
	public DB getDB(String dbname) {

		if(!opendbs.containsKey(dbname)) {
		
			// create if necessary
			dbScanner.setRange(new Range(dbname));
			Iterator<Entry<Key, Value>> iter = dbScanner.iterator();
			if(!iter.hasNext()) {
				Mutation m = new Mutation(dbname);
				try {
					m.put("INFO", "creation", new Value(Value.longToBytes(System.currentTimeMillis())));
					dbWriter.addMutation(m);
					dbWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (MutationsRejectedException e) {
					e.printStackTrace();
				}
			}
			
			DB db = new DB(dbname, this);
			opendbs.put(dbname, db);
		}
		
		return opendbs.get(dbname);
	}

	
	public Collection<DB> getUsedDatabases() {

		return opendbs.values();
	}

	
	public List<String> getDatabaseNames() throws MongoException {
		ArrayList<String> dbs = new ArrayList<String>();
		dbScanner.fetchColumnFamily(new Text("INFO"));
		
		for(Entry<Key, Value> e : dbScanner) {
			dbs.add(e.getKey().getRow().toString());
		}
		
		return dbs;
	}

	
	public void dropDatabase(String dbName) throws MongoException {
		
		DB db = getDB(dbName);
		db.dropDatabase();
	}

	
	public String getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String debugString() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String getConnectPoint() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBTCPConnector getConnector() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public ReplicaSetStatus getReplicaSetStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public ServerAddress getAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public List<ServerAddress> getAllAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public List<ServerAddress> getServerAddressList() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
	public WriteConcern getWriteConcern() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public ReadPreference getReadPreference() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void addOption(int option) {
		// TODO Auto-generated method stub
		
	}

	
	public int getOptions() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public MongoOptions getMongoOptions() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public int getMaxBsonObjectSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public CommandResult fsync(boolean async) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult fsyncAndLock() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void setWriteConcern(WriteConcern concern) {
		// TODO Auto-generated method stub
		
	}

	
	public void setReadPreference(ReadPreference preference) {
		// TODO Auto-generated method stub
		
	}

	
	public void slaveOk() {
		// TODO Auto-generated method stub
		
	}

	
	public void setOptions(int options) {
		// TODO Auto-generated method stub
		
	}

	
	public void resetOptions() {
		// TODO Auto-generated method stub
		
	}

	
	public DBObject unlock() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public boolean isLocked() {
		// TODO Auto-generated method stub
		return false;
	}
	
	
}
