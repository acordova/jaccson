package com.jaccson.mongo;

import java.util.List;

import com.mongodb.CommandResult;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoder;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DBObject;
import com.mongodb.GroupCommand;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class DBCollection {

	public WriteResult insert(DBObject[] arr, WriteConcern concern)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult insert(DBObject[] arr, WriteConcern concern,
			DBEncoder encoder) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult insert(DBObject o, WriteConcern concern)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult insert(DBObject... arr) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult insert(WriteConcern concern, DBObject... arr)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult insert(List<DBObject> list) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult insert(List<DBObject> list, WriteConcern concern)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi, WriteConcern concern) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi, WriteConcern concern, DBEncoder encoder)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult update(DBObject q, DBObject o) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult updateMulti(DBObject q, DBObject o)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult remove(DBObject o, WriteConcern concern)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult remove(DBObject o, WriteConcern concern,
			DBEncoder encoder) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public WriteResult remove(DBObject o) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBCursor find(DBObject query, DBObject fields, int numToSkip,
			int batchSize, int options) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBCursor find(DBObject query, DBObject fields, int numToSkip,
			int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject findOne(Object obj) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject findOne(Object obj, DBObject fields) {
		// TODO Auto-generated method stub
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

	
	public void createIndex(DBObject keys) throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void createIndex(DBObject keys, DBObject options)
			throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void createIndex(DBObject keys, DBObject options, DBEncoder encoder)
			throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void ensureIndex(String name) {
		// TODO Auto-generated method stub
		
	}

	
	public void ensureIndex(DBObject keys) throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void ensureIndex(DBObject keys, String name) throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void ensureIndex(DBObject keys, String name, boolean unique)
			throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void ensureIndex(DBObject keys, DBObject optionsIN)
			throws MongoException {
		// TODO Auto-generated method stub
		
	}

	
	public void resetIndexCache() {
		// TODO Auto-generated method stub
		
	}

	
	public void setHintFields(List<DBObject> lst) {
		// TODO Auto-generated method stub
		
	}

	
	public DBCursor find(DBObject ref) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBCursor find(DBObject ref, DBObject keys) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBCursor find() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject findOne() throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject findOne(DBObject o) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject findOne(DBObject o, DBObject fields) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject findOne(DBObject o, DBObject fields, ReadPreference readPref) {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		
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

	
	public com.mongodb.DBCollection rename(String newName)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public com.mongodb.DBCollection rename(String newName, boolean dropTarget)
			throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject group(DBObject key, DBObject cond, DBObject initial,
			String reduce) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject group(DBObject key, DBObject cond, DBObject initial,
			String reduce, String finalize) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject group(GroupCommand cmd) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DBObject group(DBObject args) throws MongoException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public List distinct(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	
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
		// TODO Auto-generated method stub
		return false;
	}

	
	public com.mongodb.DBCollection getCollection(String n) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String getFullName() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public DB getDB() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void setObjectClass(Class c) {
		// TODO Auto-generated method stub
		
	}

	
	public Class getObjectClass() {
		// TODO Auto-generated method stub
		return null;
	}

	
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
	
}
