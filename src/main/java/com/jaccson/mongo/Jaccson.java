package com.jaccson.mongo;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
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
	
	public Jaccson(String zkServers, String instance, String user, String pass, String auths) throws AccumuloException, AccumuloSecurityException {
		
		username = user;
		password = pass;
		
		inst = new ZooKeeperInstance(instance, zkServers);
		conn = inst.getConnector(user, pass);
		
		if(auths == null || auths.equals(""))
			this.auths = new Authorizations();
		else
			this.auths = new Authorizations(auths.split(","));
	}
	
	
	
	public DB getDB(String dbname) {

		return new DB(dbname, this);
	}

	
	public Collection<DB> getUsedDatabases() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public List<String> getDatabaseNames() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void dropDatabase(String dbName) throws MongoException {
		// TODO Auto-generated method stub
		
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
