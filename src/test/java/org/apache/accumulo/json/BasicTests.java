package org.apache.accumulo.json;

import junit.framework.TestCase;


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JaccsonCursor;
import com.jaccson.JaccsonConnection;
import com.jaccson.JaccsonTable;

public class BasicTests extends TestCase {


	public void testPutGet() {

		try {

			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("putGetTestTable");

			JaccsonTable table = conn.getTable("putGetTestTable");
			table.insert("{_id:'123', field:'abc', amount:3}");
			table.flush();

			JSONObject o = table.get("123");
			System.out.println(o);

			table.close();
			conn.dropTable("putGetTestTable");
			assertTrue(o.toString().equals("{\"amount\":3,\"field\":\"abc\",\"_id\":\"123\"}"));

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} 
	}

	public void testInsertSpeed() {

		int n = 100000;

		try {
			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("insertTestTable");
			JaccsonTable table = conn.getTable("insertTestTable");

			long start = System.currentTimeMillis(); 
			for(int i=0; i < n; i++) {
				table.insert("{field:'" + i + "', amount:" + i + "}");
			}
			table.flush();
			double elapsed = System.currentTimeMillis() - start;
			elapsed /= 1000.0;
			System.out.println("wrote " + ((double)n / elapsed) + " docs per second");

			conn.dropTable("insertTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	public void testCreateIndexEmptyTable() {

		try {

			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("indexEmptyTestTable");
			JaccsonTable table = conn.getTable("indexEmptyTestTable");
			table.ensureIndex("field");

			System.out.println("done indexing");

			table.insert("{field:'aaa', amount:2}");
			table.insert("{field:'aaa', amount:6}");
			table.insert("{field:'bbb', amount:2}");
			table.insert("{field:'ccc', amount:2}");
			table.insert("{field:'ddd', amount:2}");
			table.insert("{field:'eee', amount:2}");
			table.insert("{field:'fff', amount:2}");
			table.insert("{field:'ggg', amount:2}");

			// query by index

			JaccsonCursor cursor = table.find("{field:'aaa'}", "");
			int count = 0;
			for(JSONObject o : cursor) {
				System.out.println(o);
				count++;
			}

			assertEquals(count, 2);

			conn.dropTable("indexEmptyTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} 
	}

	/**
	 * query on unindexed fields
	 */
	public void testUnindexedQuery() {


		try {
			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("indexTestTable");
			JaccsonTable table = conn.getTable("indexTestTable");


			System.out.println("done indexing");

			table.insert("{field:'aaa', amount:2}");
			table.insert("{field:'aaa', amount:6}");
			table.insert("{field:'bbb', amount:2}");
			table.insert("{field:'ccc', amount:2}");
			table.insert("{field:'ddd', amount:2}");
			table.insert("{field:'eee', amount:2}");
			table.insert("{field:'fff', amount:2}");
			table.insert("{field:'ggg', amount:2}");


			// query by index

			JaccsonCursor cursor = table.find("{field:'aaa'}", "");
			int count = 0;
			for(JSONObject o : cursor) {
				System.out.println(o);
				count++;
			}

			assertEquals(count, 2);

			table.insert("{book: {title:'java', author:'me'}, price: 100.0}");

			// test inner doc
			cursor = table.find("{book.author:'me'}", "");
			count = 0;
			for(JSONObject o : cursor) {
				System.out.println(o);
				count++;
			}

			assertEquals(count, 1);

			// test through array

			table.insert("{planes:[{model:'a',fuel:30},{model:'b',fuel:40}]}");


			cursor = table.find("{planes.fuel: 40}", "");
			count = 0;
			for(JSONObject o : cursor) {
				System.out.println();
				count++;
			}

			//table.ensureIndex("field");

			//

			conn.dropTable("indexTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} 
	}

	public void testFilteredFullTableScan() {

	}

	public void testIndexUsage() {

	}

	public void testIndexSubobject() {

	}

	public void testIndexArray() {

	}



	public void testUpdateInc() {

		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");


			conn.dropTable("updateInc");
			JaccsonTable table = conn.getTable("updateInc");

			// test with no previous object
			table.update("{_id:333}", "{newfield:1}");
			//table.findOne("{incrtest:1}")

			// test with prev object

			// test negative incr

			// test overwrite


			conn.dropTable("indexTestTable");

		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

