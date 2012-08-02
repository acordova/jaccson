package com.jaccson;

import junit.framework.TestCase;

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

			Jaccson j = new Jaccson("localhost", "acc", "root", "secret", "");

			DB db = j.getDB("test");
			DBCollection coll = db.createCollection("putGetSelect", null);
			
			coll.insert("{\"_id\":\"123\", \"field\":\"abc\", \"amount\":3}");
			coll.flush();

			DBObject o = coll.get("123", "{\"field\":1}");
			System.out.println(o);

			coll.drop();
			assertTrue(o.toString().equals("{ \"field\" : \"abc\" , \"_id\" : \"123\"}"));

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}

	public void testFindSelect() {
		try {

			Jaccson conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			

			DBCollection coll = db.getCollection("findSelectTable");
			coll.insert("{\"_id\":\"123\", \"field\":\"abc\", \"amount\":3}");
			coll.flush();

			DBCursor cur = coll.find("{\"_id\":\"123\"}", "{\"field\":1}");
			DBObject o = cur.next();

			coll.drop(); //("findSelectTable");
			assertTrue(o.toString().equals("{ \"field\" : \"abc\" , \"_id\" : \"123\"}"));

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}
	
	// TODO: write a tests for trying to update _id

	/*	public void testInsertSpeed() {

		int n = 100000;

		try {
			Jaccson conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			coll.drop(); //("insertTestTable");
			DBCollection coll = db.getCollection("insertTestTable");

			long start = System.currentTimeMillis(); 
			for(int i=0; i < n; i++) {
				coll.insert("{field:\"" + i + "\", amount:" + i + "}");
			}
			coll.flush();
			double elapsed = System.currentTimeMillis() - start;
			elapsed /= 1000.0;
			System.out.println("wrote " + ((double)n / elapsed) + " docs per second");

			coll.drop(); //("insertTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}*/
	
	private void insertsAndQueries(DBCollection coll) throws Exception {
		
		coll.insert("{\"field\":\"aaa\", \"amount\":\"2}");
		coll.insert("{\"field\":\"aaa\", \"amount\":6}");
		coll.insert("{\"field\":\"bbb\", \"amount\":2}");
		coll.insert("{\"field:\"ccc\", \"amount\":2}");
		coll.insert("{\"field:\"ddd\", \"amount\":2}");
		coll.insert("{\"field:\"eee\", \"amount\":2}");
		coll.insert("{\"field:\"fff\", \"amount\":2}");
		coll.insert("{\"field:\"ggg\", \"amount\":2}");


		DBCursor cursor = coll.find("{\"field\":\"aaa\"}");
		int count = 0;
		for(DBObject o : cursor) {
			System.out.println(o);
			count++;
		}
		assertEquals(count, 2);

		cursor = coll.find("{\"field\":\"bbb\"}");
		count = 0;
		for(DBObject o : cursor) {
			System.out.println(o);
			count++;
		}
		assertEquals(count, 1);

		cursor = coll.find("{\"field\":\"ggg\"}");
		count = 0;
		for(DBObject o : cursor) {
			System.out.println(o);
			count++;
		}

		assertEquals(count, 1);
	}

	public void testCreateIndexEmptyTable() {

		try {

			Jaccson conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			
			//coll.drop(); //("indexEmptyTestTable");
			DBCollection coll = db.getCollection("indexEmptyTestTable");
			coll.ensureIndex("{\"field\":1}");

			System.out.println("done indexing");

			insertsAndQueries(coll);

			coll.drop(); //("indexEmptyTestTable");

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
			Jaccson conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			
			//coll.drop(); //("unindexedTestTable");
			DBCollection coll = db.getCollection("unindexedTestTable");


			insertsAndQueries(coll);


			coll.drop(); //("unindexedTestTable");

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
	}


	private void insertQuerySubObject(DBCollection coll) throws Exception {
		coll.insert("{\"book\": {\"title\":\"java\", \"author\":\"me\"}, \"price\": 100.0}");
		coll.insert("{\"book\": {\"title\":\"java\", \"author\":\"bob\"}, \"price\": 100.0}");
		coll.insert("{\"book\": {\"title\":\"java\", \"author\":\"joe\"}, \"price\": 100.0}");

		// test inner doc
		DBCursor cursor = coll.find("{\"book.author\":\"me\"}");
		int count = 0;
		for(DBObject o : cursor) {
			System.out.println(o);
			count++;
		}

		assertEquals(count, 1);
	}
	
	public void testIndexSubobject() {

		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			


			DBCollection coll = db.getCollection("indexSubObject");

			coll.ensureIndex("{\"book.author\":1}");
			insertQuerySubObject(coll);
			
			coll.drop(); //("indexSubObject");			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	private void insertQueryArray(DBCollection coll) throws Exception{
		coll.insert("{\"planes\":[{\"model\":\"a\",\"fuel\":20},{\"model\":\"b\",\"fuel\":40}]}");
		coll.insert("{\"planes\":[{\"model\":\"c\",\"fuel\":30},{\"model\":\"d\",\"fuel\":50}]}");
		coll.insert("{\"planes\":[{\"model\":\"e\",\"fuel\":40},{\"model\":\"f\",\"fuel\":60}]}");
		coll.insert("{\"planes\":[{\"model\":\"g\",\"fuel\":50},{\"model\":\"h\",\"fuel\":70}]}");

		DBCursor cursor = coll.find("{\"planes.fuel\": 40}");
		int count = 0;
		for(DBObject o : cursor) {
			System.out.println(o);
			count++;
		}

		assertEquals(count, 2);
	}
	
	public void testIndexArray() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
			//coll.drop(); //("indexArray");
			DBCollection coll = db.getCollection("indexArray");
			
			coll.ensureIndex("{\"planes.fuel\":1}");
			insertQueryArray(coll);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}



	public void testUpdateNonExistInc() {

		Jaccson conn;
		try {
			conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			

			//coll.drop(); //("updateNEInc");
			DBCollection coll = db.getCollection("updateNEInc");

			// test with no previous object
			coll.update("{\"_id\":\"333\"}", "{\"$inc\":{\"newfield\":1}}");
			DBObject obj = coll.findOne("{\"_id\":\"333\"}");
			coll.drop();
			
			assertTrue(obj.get("newfield").equals(1L));

		} catch (Exception e) {
			
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateExistInc() {

		Jaccson conn;
		try {
			conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			

			//coll.drop(); //("updateInc");
			DBCollection coll = db.getCollection("updateInc");


			// test with prev object
			coll.insert("{\"_id\":\"333\", \"newfield\":1}");

			coll.update("{\"_id\":\"333\"}", "{\"$inc\":{\"newfield\":1}}");
			DBObject obj = coll.findOne("{\"_id\":\"333\"}");
			assertTrue(obj.get("newfield").equals(2L));

			// test with larger amount
			coll.update("{\"_id\":\"333\"}", "{\"$inc\":{\"newfield\":14}}");
			obj = coll.findOne("{\"_id\":\"333\"}");
			assertTrue(obj.get("newfield").equals(16L));

			// test negative incr
			coll.update("{\"_id\":\"333\"}", "{\"$inc\":{\"newfield\":-7}}");
			obj = coll.findOne("{\"_id\":\"333\"}");
			assertTrue(obj.get("newfield").equals(9L));


			// test overwrite
			coll.insert("{\"_id\":\"333\", \"newfield\":0}");

			obj = coll.findOne("{\"_id\":\"333\"}");
			assertTrue(obj.get("newfield").equals(0L));


			coll.drop(); //("indexTestTable");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateSet() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			

			//coll.drop(); //("updateSet");
			DBCollection coll = db.getCollection("updateSet");

			coll.insert("{\"_id\":\"334\", \"x\":5}");

			coll.update("{\"_id\":\"334\"}", "{\"$set\":{\"x\":\"a\"}}");

			DBObject obj = coll.findOne("{\"_id\":\"334\"}");
			assertTrue(obj.get("x").equals("a"));

			coll.drop(); //("updateSet");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateNonexistSet() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			

			//coll.drop(); //("updateNESet");
			DBCollection coll = db.getCollection("updateNESet");

			// this should act the same as 
			// table.insert("{\"_id\":334, x:\"a\"}");
			coll.update("{\"_id\":\"334\"}", "{\"$set\":{\"x\":\"a\"}}");

			DBObject obj = coll.findOne("{\"_id\":\"334\"}");
			assertTrue(obj.get("x").equals("a"));

			coll.drop(); //("updateNESet");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 
	}

	public void testUpdateUnset() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost", "acc", "root", "secret", "");
			DB db = conn.getDB("test");
			

			//coll.drop(); //("updateUnset");
			DBCollection coll = db.getCollection("updateUnset");

			coll.insert("{\"_id\":\"334\", \"x\":5}");

			coll.update("{\"_id\":\"334\"}", "{\"$unset\":\"x\"}");

			DBObject obj = coll.findOne("{\"_id\":\"334\"}");
			assertTrue(obj.get("x") == null);

			coll.drop();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePush() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePushAll() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdateAddToSet() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdateEach() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePop() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		fail();
	}

	public void testUpdatePull() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 

		fail();
	}

	public void testUpdatePullAll() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 

		fail();
	}

	public void testUpdateRename() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 


		fail();
	}

	public void testUpdateBit() {
		Jaccson conn;
		try {
			conn = new Jaccson("localhost","acc","root","secret","");
			DB db = conn.getDB("test");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} 

		fail();
	}
}

