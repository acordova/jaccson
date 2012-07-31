/**
 * thin wrapper for Jaccson object
 * 
 * this exists to allow existing mongo clients to not have to change to use the Jaccson object
 * 
 */
package com.jaccson;

import java.util.Collection;
import java.util.List;

import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.DBTCPConnector;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class Mongo {

	Jaccson jaccson;

	
	
	public DB getDB(String dbname) {
		return jaccson.getDB(dbname);
	}

	public Collection<DB> getUsedDatabases() {
		return jaccson.getUsedDatabases();
	}

	public List<String> getDatabaseNames() throws MongoException {
		return jaccson.getDatabaseNames();
	}

	public void dropDatabase(String dbName) throws MongoException {
		jaccson.dropDatabase(dbName);
	}

	public String getVersion() {
		return jaccson.getVersion();
	}

	public String debugString() {
		return jaccson.debugString();
	}

	public String getConnectPoint() {
		return jaccson.getConnectPoint();
	}

	public DBTCPConnector getConnector() {
		return jaccson.getConnector();
	}

	public ReplicaSetStatus getReplicaSetStatus() {
		return jaccson.getReplicaSetStatus();
	}

	public ServerAddress getAddress() {
		return jaccson.getAddress();
	}

	public List<ServerAddress> getAllAddress() {
		return jaccson.getAllAddress();
	}

	public List<ServerAddress> getServerAddressList() {
		return jaccson.getServerAddressList();
	}

	public void close() {
		jaccson.close();
	}

	public WriteConcern getWriteConcern() {
		return jaccson.getWriteConcern();
	}

	public ReadPreference getReadPreference() {
		return jaccson.getReadPreference();
	}

	public void addOption(int option) {
		jaccson.addOption(option);
	}

	public boolean equals(Object obj) {
		return jaccson.equals(obj);
	}

	public int getOptions() {
		return jaccson.getOptions();
	}

	public MongoOptions getMongoOptions() {
		return jaccson.getMongoOptions();
	}

	public int getMaxBsonObjectSize() {
		return jaccson.getMaxBsonObjectSize();
	}

	public CommandResult fsync(boolean async) {
		return jaccson.fsync(async);
	}

	public CommandResult fsyncAndLock() {
		return jaccson.fsyncAndLock();
	}

	public int hashCode() {
		return jaccson.hashCode();
	}

	public void setWriteConcern(WriteConcern concern) {
		jaccson.setWriteConcern(concern);
	}

	public void setReadPreference(ReadPreference preference) {
		jaccson.setReadPreference(preference);
	}

	public void slaveOk() {
		jaccson.slaveOk();
	}

	public void setOptions(int options) {
		jaccson.setOptions(options);
	}

	public void resetOptions() {
		jaccson.resetOptions();
	}

	public boolean isLocked() {
		return jaccson.isLocked();
	}

	public String toString() {
		return jaccson.toString();
	}

	public DBObject unlock() {
		return jaccson.unlock();
	}	
}
