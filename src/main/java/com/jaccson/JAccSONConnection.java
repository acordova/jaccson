package com.jaccson;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;


// TODO: create a general exception to contain all these others

public class JAccSONConnection {

	Instance inst;
	Connector conn;
	private Authorizations auths;
	
	public JAccSONConnection(String zkServers, String instance, String user, String pass, String auths) throws AccumuloException, AccumuloSecurityException {
		
		inst = new ZooKeeperInstance(instance, zkServers);
		conn = inst.getConnector(user, pass.getBytes());
		
		if(auths == null || auths.equals(""))
			this.auths = new Authorizations();
		else
			this.auths = new Authorizations(auths.split(","));
	}
	
	public JAccSONTable getTable(String table) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
		
		return new JAccSONTable(table, conn, auths);
	}
	
	public void createTable(String table) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		
		System.out.println("creating table " + table);
		conn.tableOperations().create(table);
	}
	
	public void dropTable(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		
		JAccSONTable t = new JAccSONTable(table, conn, auths);
		t.drop();
	}

	public static void main(String[] args) {
		try {
			JAccSONConnection c = new JAccSONConnection("localhost", "test", "root", "secret", "");
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
