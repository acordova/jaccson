package com.jaccson.proxy;

/**
 * This allows one to use a client in a language that doesn't run on the JVM
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import com.jaccson.DB;
import com.jaccson.DBCollection;
import com.jaccson.DBCursor;
import com.jaccson.Jaccson;
import com.mongodb.DBObject;


public class JaccsonProxy implements TableCursorService.Iface {

	private HashMap<String, DBCollection> openCollections;
	private HashMap<Integer, DBCursor> openCursors;
	private Jaccson conn;
	private static final int BATCH_SIZE = 100;
	private Random random = new Random();
	private HashMap<String, DB> openDBs;

	public JaccsonProxy(String zkServers, String instance, String user, String password, String auths) throws AccumuloException, AccumuloSecurityException {
		openCollections = new HashMap<String, DBCollection>();
		openDBs = new HashMap<String, DB>();
		openCursors = new HashMap<Integer, DBCursor>();

		conn = new Jaccson(zkServers, instance, user, password, auths);
	}
	
	@Override
	public List<String> nextBatch(int cursor) throws JaccsonException {

		DBCursor cur = openCursors.get(cursor);
		ArrayList<String> batch = new ArrayList<String>();

		while(cur.hasNext() && batch.size() < BATCH_SIZE) {
			batch.add(cur.next().toString());
		}

		if(batch.size() == 0) { // done
			openCursors.remove(cursor);
		}

		return batch;
	}

	@Override
	public void insertBatch(String db, String coll, List<String> json) throws JaccsonException {
		try {
			DBCollection t = getCollection(db, coll, true);
			
			for(String j : json)
				t.insert(j);

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		} 
	}

	@Override
	public void update(String db, String coll, String query, String mods) throws JaccsonException {
		try {
			DBCollection t = getCollection(db, coll, true);
			t.update(query, mods);
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	@Override
	public int find(String db, String coll, String query, String select)
			throws JaccsonException {

		try {
			DBCollection t = getCollection(db, coll, false);


			DBCursor cursor = t.find(query, select);
			// find a random label
			int label;

			synchronized(openCursors) {

				label = random.nextInt();

				while(openCursors.containsKey(label))
					label = random.nextInt();

				openCursors.put(label, cursor);
			}

			return label;



		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	private DB getDB(String dbname) {
		
		if(!openDBs.containsKey(dbname)) {
			DB db = conn.getDB(dbname);
			openDBs.put(dbname, db);
		}
		
		return openDBs.get(dbname); 
	}
	
	private DBCollection getCollection(String dbname, String collname, boolean create) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException {

		String fullname = dbname + "_" + collname;
		
		if(!openCollections.containsKey(fullname)) {
			DB db = getDB(dbname);
			System.err.println("opening new table " + fullname);
			DBCollection c = db.getCollection(collname);
			openCollections.put(fullname, c);
		}
		
		return openCollections.get(fullname);
	}

	@Override
	public String findOne(String db, String coll, String query, String select) throws JaccsonException {

		try {
			DBCollection t = getCollection(db, coll, false);
			DBObject o = t.findOne(query, select);
			if(o != null)
				return o.toString();

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

		return null;
	}

	@Override
	public String get(String db, String coll, String oid) throws JaccsonException {

		try {
			DBCollection t = getCollection(db, coll, false);
			DBObject o = t.get(oid, (String)null);
			if(o != null)
				return o.toString();

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

		return null;
	}

	@Override
	public void remove(String db, String coll, String query) throws JaccsonException {
		try {
			DBCollection t = getCollection(db, coll, false);
			t.remove(query);
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	public void close(String db, String coll) throws JaccsonException {
		// TODO: implement?
	}


	@Override
	public void ensureIndex(String db, String coll, String obj, boolean drop)
			throws JaccsonException, TException {
		try {
			DBCollection t = getCollection(db, coll, true);
			t.ensureIndex(obj);
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	@Override
	public void dropIndex(String db, String coll, String path) throws JaccsonException {
		try {
			DBCollection t = getCollection(db, coll, false);

			t.dropIndex(path);
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

	}

	@Override
	public void compact(String db, String coll) throws JaccsonException {
		try {
			DBCollection t = getCollection(db, coll, false);
			t.compact();
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	@Override
	public void drop(String db, String coll) throws JaccsonException {
		try {
			getCollection(db, coll, false).drop();
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	@Override
	public void flush(String db, String coll) throws JaccsonException {
		try {
			DBCollection t = getCollection(db, coll, false);
			t.flush();
			openCollections.remove(db + "_" + coll);

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}
	

	public static void main(String[] args) {

		try {
			JaccsonProxy handler = new JaccsonProxy(args[0], args[1], args[2], args[3], args[4]);

			TableCursorService.Processor processor = new TableCursorService.Processor(handler);
			TServerTransport serverTransport = new TServerSocket(Integer.parseInt(args[5]));

			TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

			// Use this for a multi threaded server
			//TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));


			System.out.println("Starting the server...");
			server.serve();

		} catch (Exception x) {
			x.printStackTrace();
		}
		System.out.println("done.");
	}
}
