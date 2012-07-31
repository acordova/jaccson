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
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import org.json.JSONObject;

import com.jaccson.deprec.JaccsonConnection;
import com.jaccson.deprec.JaccsonCursor;
import com.jaccson.deprec.JaccsonTable;

public class JaccsonProxy implements TableCursorService.Iface {

	private HashMap<String, JaccsonTable> openTables;
	private HashMap<Integer, JaccsonCursor> openCursors;
	private JaccsonConnection conn;
	private static final int BATCH_SIZE = 100;
	private Random random = new Random();

	public JaccsonProxy(String zkServers, String instance, String user, String password, String auths) throws AccumuloException, AccumuloSecurityException {
		openTables = new HashMap<String, JaccsonTable>();
		openCursors = new HashMap<Integer, JaccsonCursor>();

		conn = new JaccsonConnection(zkServers, instance, user, password, auths);
	}
	
	// cursor methods
	public List<String> nextBatch(int cursor) throws JaccsonException {

		JaccsonCursor cur = openCursors.get(cursor);
		ArrayList<String> batch = new ArrayList<String>();

		while(cur.hasNext() && batch.size() < BATCH_SIZE) {
			batch.add(cur.next().toString());
		}

		if(batch.size() == 0) { // done
			openCursors.remove(cursor);
		}

		return batch;
	}

	// table methods
	public void insertBatch(String table, List<String> json) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, true);
			
			for(String j : json)
				t.insert(new JSONObject(j));

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		} 
	}

	public void update(String table, String query, String mods) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, true);
			t.update(new JSONObject(query), new JSONObject(mods));
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	public int find(String table, String query, String select)
			throws JaccsonException {

		try {
			JaccsonTable t = getTable(table, false);


			JaccsonCursor cursor = t.find(new JSONObject(query), new JSONObject(select));
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

	private JaccsonTable getTable(String table, boolean create) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException {

		JaccsonTable t = null;
		if(!openTables.containsKey(table)) {
			System.err.println("opening new table " + table);
			t = conn.getTable(table);
			openTables.put(table, t);
		}
		else {
			t = openTables.get(table);
		}

		return t;
	}

	public String findOne(String table, String query, String select) throws JaccsonException {

		try {
			JaccsonTable t = getTable(table, false);
			JSONObject o = t.findOne(new JSONObject(query), new JSONObject(select));
			if(o != null)
				return o.toString();

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

		return null;
	}

	public String get(String table, String oid) throws JaccsonException {

		try {
			JaccsonTable t = getTable(table, false);
			JSONObject o = t.get(oid);
			if(o != null)
				return o.toString();

		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

		return null;
	}

	public void remove(String table, String query) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, false);
			t.remove(new JSONObject(query));
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	public void close(String table) throws JaccsonException {
		// TODO: implement?
	}

	public void ensureIndex(String table, String path) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, true);
			t.ensureIndex(new JSONObject(path));
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

	}

	public void dropIndex(String table, String path) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, false);

			t.dropIndex(new JSONObject(path));
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}

	}

	public void compact(String table) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, false);
			t.compact();
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	public void drop(String table) throws JaccsonException {
		try {
			conn.dropTable(table);
		} catch (Exception e) {
			throw new JaccsonException(e.getMessage());
		}
	}

	@Override
	public void flush(String table) throws JaccsonException {
		try {
			JaccsonTable t = getTable(table, false);
			t.flush();
			openTables.remove(table);

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
