package com.jaccson;

import java.util.List;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.json.JSONObject;

import com.jaccson.deprec.JaccsonConnection;
import com.jaccson.deprec.JaccsonCursor;
import com.jaccson.deprec.JaccsonTable;
import com.mongodb.DBObject;

public class BasicTests extends TestCase {
/*
	public void testNewJaccson() {
		try {
			Jaccson j = new Jaccson("localhost", "acc", "root", "secret", "");
			
			List<String> dbs = j.getDatabaseNames();
			assertTrue(dbs.size() == 0);
			
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testCreateDeleteEmptyDB() {
		try {
			Jaccson j = new Jaccson("localhost", "acc", "root", "secret", "");
			
			DB db = j.getDB("testDB");
			assertTrue(j.getDatabaseNames().size() > 0);
			
			db.dropDatabase();
			assertTrue(j.getDatabaseNames().size() == 0);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
	}
	
	public void testCreateDeleteCollection() {
		try {
			Jaccson j = new Jaccson("localhost", "acc", "root", "secret", "");
			
			DB db = j.getDB("testDB");
			assertTrue(j.getDatabaseNames().size() > 0);
			assertTrue(db.getCollectionNames().size() == 0);
			
			DBCollection coll = db.createCollection("testColl", null);
			assertTrue(db.getCollectionNames().size() == 1);
			
			coll.drop();
			assertTrue(db.getCollectionNames().size() == 0);
			
			db.dropDatabase();
			assertTrue(j.getDatabaseNames().size() == 0);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	public void testDeleteNonEmptyDatabase() {
		try {
			Jaccson j = new Jaccson("localhost", "acc", "root", "secret", "");
			
			DB db = j.getDB("testDB");
			assertTrue(j.getDatabaseNames().size() > 0);
			assertTrue(db.getCollectionNames().size() == 0);
			
			db.createCollection("testColl", null);
			assertTrue(db.getCollectionNames().size() == 1);
			
			db.dropDatabase();
			assertTrue(j.getDatabaseNames().size() == 0);
			
			assertTrue(db.dbsTable.listCollections("testDB").size() == 0);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	*/
	
	public void testPutGet() {

		try {

			Jaccson j = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = j.getDB("test");
			DBCollection coll = db.getCollection("putGet");
			coll.drop();

			coll = db.createCollection("putGet", null);
			coll.insert("{\"_id\":\"123\", \"field\":\"abc\", \"amount\":3}");
			DBObject o = coll.findOne("123");
			
			coll.close();
			coll.drop();

			System.out.println(o);
			assertTrue(o.toString().equals("{ \"amount\" : 3 , \"field\" : \"abc\" , \"_id\" : \"123\"}"));

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}
	
	public void testPutGetSelect() {
		try {

			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("putGetSelectTable");

			JaccsonTable table = conn.getTable("putGetSelectTable");
			table.insert("{_id:'123', field:'abc', amount:3}");
			table.flush();

			JSONObject o = table.get("123", "{field:1}");
			System.out.println(o);

			table.close();
			conn.dropTable("putGetTestTable");
			assertTrue(o.toString().equals("{\"field\":\"abc\",\"_id\":\"123\"}"));

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}

	public void testFindSelect() {
		try {

			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("findSelectTable");

			JaccsonTable table = conn.getTable("findSelectTable");
			table.insert("{_id:'123', field:'abc', amount:3}");
			table.flush();

			JaccsonCursor cur = table.find("{_id:'123'}", "{field:1}");
			JSONObject o = cur.next();

			conn.dropTable("findSelectTable");
			assertTrue(o.toString().equals("{\"field\":\"abc\",\"_id\":\"123\"}"));

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}
	
	// TODO: write a tests for trying to update _id

	/*	public void testInsertSpeed() {

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
			fail();
		}
	}*/
	
	private void insertsAndQueries(JaccsonTable table) throws Exception {
		
		table.insert("{field:'aaa', amount:2}");
		table.insert("{field:'aaa', amount:6}");
		table.insert("{field:'bbb', amount:2}");
		table.insert("{field:'ccc', amount:2}");
		table.insert("{field:'ddd', amount:2}");
		table.insert("{field:'eee', amount:2}");
		table.insert("{field:'fff', amount:2}");
		table.insert("{field:'ggg', amount:2}");


		JaccsonCursor cursor = table.find("{field:'aaa'}", "");
		int count = 0;
		for(JSONObject o : cursor) {
			System.out.println(o);
			count++;
		}
		assertEquals(count, 2);

		cursor = table.find("{field:'bbb'}", "");
		count = 0;
		for(JSONObject o : cursor) {
			System.out.println(o);
			count++;
		}
		assertEquals(count, 1);

		cursor = table.find("{field:'ggg'}", "");
		count = 0;
		for(JSONObject o : cursor) {
			System.out.println(o);
			count++;
		}

		assertEquals(count, 1);
	}

	public void testCreateIndexEmptyTable() {

		try {

			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("indexEmptyTestTable");
			JaccsonTable table = conn.getTable("indexEmptyTestTable");
			table.ensureIndex("{field:1}");

			System.out.println("done indexing");

			insertsAndQueries(table);

			conn.dropTable("indexEmptyTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}

	/**
	 * query on unindexed fields
	 */
	public void testUnindexedQuery() {

		try {
			JaccsonConnection conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("unindexedTestTable");
			JaccsonTable table = conn.getTable("unindexedTestTable");


			insertsAndQueries(table);


			conn.dropTable("unindexedTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}


	private void insertQuerySubObject(JaccsonTable table) throws Exception {
		table.insert("{book: {title:'java', author:'me'}, price: 100.0}");
		table.insert("{book: {title:'java', author:'bob'}, price: 100.0}");
		table.insert("{book: {title:'java', author:'joe'}, price: 100.0}");

		// test inner doc
		JaccsonCursor cursor = table.find("{book.author:'me'}", "");
		int count = 0;
		for(JSONObject o : cursor) {
			System.out.println(o);
			count++;
		}

		assertEquals(count, 1);
	}
	
	public void testIndexSubobject() {

		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");


			conn.dropTable("indexSubObject");
			JaccsonTable table = conn.getTable("indexSubObject");

			table.ensureIndex("{book.author:1}");
			insertQuerySubObject(table);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	private void insertQueryArray(JaccsonTable table) throws Exception{
		table.insert("{planes:[{model:'a',fuel:20},{model:'b',fuel:40}]}");
		table.insert("{planes:[{model:'c',fuel:30},{model:'d',fuel:50}]}");
		table.insert("{planes:[{model:'e',fuel:40},{model:'f',fuel:60}]}");
		table.insert("{planes:[{model:'g',fuel:50},{model:'h',fuel:70}]}");

		JaccsonCursor cursor = table.find("{planes.fuel: 40}", "");
		int count = 0;
		for(JSONObject o : cursor) {
			System.out.println(o);
			count++;
		}

		assertEquals(count, 2);
	}
	
	public void testIndexArray() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");

			conn.dropTable("indexArray");
			JaccsonTable table = conn.getTable("indexArray");
			
			table.ensureIndex("{planes.fuel:1}");
			insertQueryArray(table);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}



	public void testUpdateNonExistInc() {

		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");


			conn.dropTable("updateNEInc");
			JaccsonTable table = conn.getTable("updateNEInc");

			// test with no previous object
			table.update("{_id:333}", "{$inc:{newfield:1}}");
			JSONObject obj = table.findOne("{_id:333}");
			table.drop();
			
			assertTrue(obj.getInt("newfield") == 1);

		} catch (Exception e) {
			
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateExistInc() {

		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");


			conn.dropTable("updateInc");
			JaccsonTable table = conn.getTable("updateInc");


			// test with prev object
			table.insert("{_id:333, newfield:1}");

			table.update("{_id:333}", "{$inc:{newfield:1}}");
			JSONObject obj = table.findOne("{_id:333}");
			assertTrue(obj.getInt("newfield") == 2);

			// test with larger amount
			table.update("{_id:333}", "{$inc:{newfield:14}}");
			obj = table.findOne("{_id:333}");
			assertTrue(obj.getInt("newfield") == 16);

			// test negative incr
			table.update("{_id:333}", "{$inc:{newfield:-7}}");
			obj = table.findOne("{_id:333}");
			assertTrue(obj.getInt("newfield") == 9);


			// test overwrite
			table.insert("{_id:333, newfield:0}");

			obj = table.findOne("{_id:333}");
			assertTrue(obj.getInt("newfield") == 0);


			conn.dropTable("indexTestTable");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateSet() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");


			conn.dropTable("updateSet");
			JaccsonTable table = conn.getTable("updateSet");

			table.insert("{_id:'334', x:5}");

			table.update("{_id:'334'}", "{$set:{x:'a'}}");

			JSONObject obj = table.findOne("{_id:'334'}");
			assertTrue(obj.getString("x").equals("a"));

			conn.dropTable("updateSet");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateNonexistSet() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");


			conn.dropTable("updateNESet");
			JaccsonTable table = conn.getTable("updateNESet");

			// this should act the same as 
			// table.insert("{_id:334, x:'a'}");
			table.update("{_id:'334'}", "{$set:{x:'a'}}");

			JSONObject obj = table.findOne("{_id:'334'}");
			assertTrue(obj.getString("x").equals("a"));

			conn.dropTable("updateNESet");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateUnset() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");


			conn.dropTable("updateUnset");
			JaccsonTable table = conn.getTable("updateUnset");

			table.insert("{_id:334, x:5}");

			table.update("{_id:334}", "{$unset:x}");

			JSONObject obj = table.findOne("{_id:334}");
			assertTrue(obj.getString("x") == null);

			conn.dropTable("updateUnset");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePush() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePushAll() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdateAddToSet() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdateEach() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePop() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePull() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 

		fail();
	}

	public void testUpdatePullAll() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 

		fail();
	}

	public void testUpdateRename() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 


		fail();
	}

	public void testUpdateBit() {
		JaccsonConnection conn;
		try {
			conn = new JaccsonConnection("localhost","acc","root","secret","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 

		fail();
	}
}

