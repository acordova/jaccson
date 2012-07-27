package com.jaccson.mongo;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.mongodb.CommandResult;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class DB {

	String name;
	Jaccson jaccson;


	DB(String name, Jaccson jaccson) {
		this.name = name;
		this.jaccson = jaccson;
	}
	
	public DBCollection createCollection(String coll, DBObject options) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

		return new DBCollection(coll, this);
	}

	public CommandResult command(DBObject cmd) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(DBObject cmd, DBEncoder encoder)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(DBObject cmd, int options, DBEncoder encoder)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(DBObject cmd, int options,
			ReadPreference readPrefs) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(DBObject cmd, int options,
			ReadPreference readPrefs, DBEncoder encoder) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(DBObject cmd, int options)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(String cmd) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult command(String cmd, int options) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult doEval(String code, Object... args)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult getStats() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public boolean collectionExists(String collectionName) {
		// TODO Auto-generated method stub
		return false;
	}

	
	public CommandResult getLastError() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult getLastError(WriteConcern concern)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult getLastError(int w, int wtimeout, boolean fsync)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteConcern getWriteConcern() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public ReadPreference getReadPreference() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void dropDatabase() throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public boolean authenticate(String username, char[] passwd)
			throws MongoException {
		// TODO Auto-generated method stub
		return false;
	}

	
	public CommandResult authenticateCommand(String username, char[] passwd)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult addUser(String username, char[] passwd) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult addUser(String username, char[] passwd, boolean readOnly) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CommandResult getPreviousError() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public Jaccson getMongo() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DB getSisterDB(String name) {
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

	
	public void cleanCursors(boolean force) throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public DBCollection getCollection(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBCollection getCollectionFromString(String s) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public Object eval(String code, Object... args) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public Set<String> getCollectionNames() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void forceError() throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void requestStart() {
		// TODO Auto-generated method stub
		
	}

	
	public void requestDone() {
		// TODO Auto-generated method stub
		
	}

	
	public void requestEnsureConnection() {
		// TODO Auto-generated method stub
		
	}

	
	public void setReadOnly(Boolean b) {
		// TODO Auto-generated method stub
		
	}

	
	public void setWriteConcern(WriteConcern concern) {
		// TODO Auto-generated method stub
		
	}

	
	public void setReadPreference(ReadPreference preference) {
		// TODO Auto-generated method stub
		
	}

	
	public boolean isAuthenticated() {
		// TODO Auto-generated method stub
		return false;
	}

	
	public WriteResult removeUser(String username) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void resetError() throws MongoException {
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
	
}
