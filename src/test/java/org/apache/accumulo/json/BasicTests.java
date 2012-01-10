package org.apache.accumulo.json;

import junit.framework.TestCase;
import org.json.JSONObject;

import com.jaccson.JAccSONConnection;
import com.jaccson.JAccSONTable;

public class BasicTests extends TestCase {

	public void testPutGet() {

		try {

			JAccSONConnection conn = new JAccSONConnection("localhost", "acc", "root", "secret", "");

			conn.createTable("testTable");

			JAccSONTable table = conn.getTable("testTable");
			table.insert("{_id:'123', field:'abc', amount:3}");
			table.flush();

			JSONObject o = table.get("123");
			System.out.println(o);

			table.close();
			conn.dropTable("testTable");
			assertTrue(o.toString().equals("{\"amount\":3,\"field\":\"abc\",\"_id\":\"123\"}"));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		} 
	}

	public void testInsertSpeed() {

		int n = 100000;

		try {
			JAccSONConnection conn = new JAccSONConnection("localhost", "acc", "root", "secret", "");

			conn.createTable("testTable");

			JAccSONTable table = conn.getTable("testTable");

			long start = System.currentTimeMillis(); 
			for(int i=0; i < n; i++) {
				table.insert("{field:'" + i + "', amount:" + i + "}");
			}
			table.flush();
			long elapsed = System.currentTimeMillis() - start;
			elapsed /= 1000;
			System.out.println((double)n / elapsed);

			conn.dropTable("testTable");
			
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	public void testCreateEmptyIndex() {
		
		try {

			JAccSONConnection conn = new JAccSONConnection("localhost", "acc", "root", "secret", "");

			conn.createTable("testTable");
			JAccSONTable table = conn.getTable("testTable");
			table.ensureIndex("field");
			
			table.insert("{field:'aaa', amount:2}");
			table.insert("{field:'bbb', amount:2}");
			table.insert("{field:'ccc', amount:2}");
			table.insert("{field:'ddd', amount:2}");
			table.insert("{field:'eee', amount:2}");
			table.insert("{field:'fff', amount:2}");
			table.insert("{field:'ggg', amount:2}");
			
			//table.dropIndex("field");
			//conn.dropTable("testTable");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		} 
	}

	public void testBuildIndex() {

	}

	public void testIndexUsage() {

	}
	
	public void testIndexSubobject() {
		
	}
	
	public void testIndexArray() {
		
	}
	
	public void testUpdateInc() {
		
	}
	
	public void testUpdateSet() {
		
	}
	
	public void testUpdateUnset() {
		
	}
	
	public void testUpdatePush() {
		
	}
	
	public void testUpdatePushAll() {
		
	}
	
	public void testUpdateAddToSet() {
		
	}
	
	public void testUpdateEach() {
		
	}
	
	public void testUpdatePop() {
		
	}
	
	public void testUpdatePull() {
		
	}
	
	public void testUpdatePullAll() {
		
	}
	
	public void testUpdateRename() {
		
	}
	
	public void testUpdateBit() {
		
	}
}

