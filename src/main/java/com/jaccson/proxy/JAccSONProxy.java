/**
 * This allows one to use a client in a language that doesn't run on the JVM
 */
package com.jaccson.proxy;

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
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JAccSONConnection;
import com.jaccson.JAccSONCursor;
import com.jaccson.JAccSONTable;

public class JAccSONProxy implements TableCursorService.Iface {

	private HashMap<String, JAccSONTable> openTables;
	private HashMap<Integer, JAccSONCursor> openCursors;
	private JAccSONConnection conn;
	private static final int BATCH_SIZE = 100;
	private Random random = new Random();

	public JAccSONProxy(String instance, String zkServers, String user, String password, String auths) throws AccumuloException, AccumuloSecurityException {
		openTables = new HashMap<String, JAccSONTable>();

		conn = new JAccSONConnection(instance, zkServers, user, password, auths);
	}

	// cursor methods
	public List<String> nextBatch(int cursor) throws TException {

		JAccSONCursor cur = openCursors.get(cursor);
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
	public void insert(String table, String json) throws TException {
		try {
			JAccSONTable t = getTable(table, true);
			t.insert(json);

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public void update(String table, String query, String mods)
			throws TException {
		try {
			JAccSONTable t = getTable(table, true);
			t.update(query, mods);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int find(String table, String query, String select)
			throws TException {

		try {
			JAccSONTable t = getTable(table, false);

			try {
				JAccSONCursor cursor = t.find(query, select);
				// find a random label
				int label;
				
				synchronized(openCursors) {
					
					label = random.nextInt();
					
					while(openCursors.containsKey(label))
						label = random.nextInt();
					
					openCursors.put(label, cursor);
				}
				
				return label;
				
			} catch (JSONException e) {
				e.printStackTrace();
			}

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		}

		return -1;
	}

	private JAccSONTable getTable(String table, boolean create) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException {

		JAccSONTable t = null;
		if(!openTables.containsKey(table)) {
			try {
				t = conn.getTable(table);
			} catch (TableNotFoundException e) {
				if(create) {
					conn.createTable(table);
					t = conn.getTable(table);
				}
				else {
					throw e;
				}
			}
		}
		else {
			t = openTables.get(table);
		}

		return t;
	}

	public String findOne(String table, String query, String select) throws TException {

		try {
			JAccSONTable t = getTable(table, false);
			JSONObject o = t.findOne(query, select);
			if(o != null)
				return o.toString();
			
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	public String get(String table, String oid) throws TException {
		
		try {
			JAccSONTable t = getTable(table, false);
			JSONObject o = t.get(oid);
			if(o != null)
				return o.toString();
			
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	public void remove(String table, String query) throws TException {
		try {
			JAccSONTable t = getTable(table, false);
			t.remove(query);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void close(String table) throws TException {

	}

	public void ensureIndex(String table, String path) throws TException {
		try {
			JAccSONTable t = getTable(table, true);
			t.ensureIndex(path);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void dropIndex(String table, String path) throws TException {
		try {
			JAccSONTable t = getTable(table, false);
			
			t.dropIndex(path);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void compact(String table) throws TException {
		// TODO Auto-generated method stub

	}

	public void drop(String table) throws TException {
		try {
			conn.dropTable(table);
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void flush(String table) throws TException {
		try {
			JAccSONTable t = getTable(table, false);
			t.flush();
			
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		try {
		      JAccSONProxy handler = new JAccSONProxy(args[1], args[2], args[3], args[4], args[5]);
		      
		      TableCursorService.Processor processor = new TableCursorService.Processor(handler);
		      TServerTransport serverTransport = new TServerSocket(Integer.parseInt(args[6]));
		      TServer server = new TSimpleServer(processor, serverTransport);

		      // Use this for a multithreaded server
		      //server = new TThreadPoolServer(processor, serverTransport);

		      System.out.println("Starting the server...");
		      server.serve();

		    } catch (Exception x) {
		      x.printStackTrace();
		    }
		    System.out.println("done.");
	}
}
