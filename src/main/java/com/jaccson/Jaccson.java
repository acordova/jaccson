package com.jaccson;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;

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
	DatabasesTable dbsTable;
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
		
		opendbs = new HashMap<String,DB>();
		dbsTable = new DatabasesTable(this);
	}
	
	
	
	public DB getDB(String dbname) {

		if(!opendbs.containsKey(dbname)) {
		
			// create if necessary
			DB db;
			try {
				db = new DB(dbname, this);
				opendbs.put(dbname, db);
			} catch (MutationsRejectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return opendbs.get(dbname);
	}

	
	public Collection<DB> getUsedDatabases() {
		return opendbs.values();
	}

	
	public List<String> getDatabaseNames() throws MongoException {
		return dbsTable.listDBs();
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
