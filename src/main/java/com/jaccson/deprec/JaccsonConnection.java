package com.jaccson.deprec;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;


// TODO: create a general exception to contain all these others

public class JaccsonConnection {

	Instance inst;
	Connector conn;
	private Authorizations auths;
	private String username;
	private String password;
	
	public JaccsonConnection(String zkServers, String instance, String user, String pass, String auths) throws AccumuloException, AccumuloSecurityException {
		
		username = user;
		password = pass;
		
		inst = new ZooKeeperInstance(instance, zkServers);
		conn = inst.getConnector(user, pass);
		
		if(auths == null || auths.equals(""))
			this.auths = new Authorizations();
		else
			this.auths = new Authorizations(auths.split(","));
	}
	
	public JaccsonTable getTable(String table) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
		
		return new JaccsonTable(table, conn, auths, username, password);
	}
		
	public void dropTable(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		
		if(conn.tableOperations().exists(table)) {
			JaccsonTable t = new JaccsonTable(table, conn, auths, username, password);
			t.drop();
		}
	}
	
	
	
}
