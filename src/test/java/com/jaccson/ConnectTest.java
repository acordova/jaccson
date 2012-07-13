package com.jaccson;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

import com.jaccson.JaccsonConnection;

public class ConnectTest {
	public static void main(String args[] ) throws AccumuloException, AccumuloSecurityException {
		JaccsonConnection conn = new JaccsonConnection("Aarons-MacBook-Air.local", "acc", "root", "secret", "");

	}
}
