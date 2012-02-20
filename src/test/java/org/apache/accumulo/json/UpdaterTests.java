package org.apache.accumulo.json;

import java.util.HashSet;

import org.json.*;

import static org.junit.Assert.*;

import org.junit.Test;

import com.jaccson.server.JaccsonUpdater;

public class UpdaterTests {

	@Test
	public void testPullAll() {
		
		try {
			
			HashSet<Integer> evens = new HashSet<Integer>();
			evens.add(2);
			evens.add(4);
			evens.add(6);
			evens.add(8);
			evens.add(10);
			
			HashSet<Integer> odds = new HashSet<Integer>();
			odds.add(1);
			odds.add(3);
			odds.add(5);
			odds.add(7);
			odds.add(9);
			odds.add(11);
			
			JSONObject o = new JSONObject("{things:[1,2,3,4,5,6,7,8,9,10,11]}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$pullAll':{things:[2,4,6,8,10]}}"), o);
			
			JSONArray things = o.getJSONArray("things");
			for(int i=0; i <  things.length(); i++) {
				if(evens.contains(things.get(i))) {
					fail();
				}
			}
			
			for(int i=0; i <  things.length(); i++) {
				if(!odds.contains(things.get(i))) {
					fail();
				}
				odds.remove(things.get(i));
			}
			
			assertEquals(odds.size(), 0);
			
		} catch (JSONException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testPull() {
		
		try {
			JSONObject o = new JSONObject("{things:[1,2,3,4,5,6,7,8,9,10,11]}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$pull':{things:1}}"), o);
						
			HashSet<Integer> left = new HashSet<Integer>();
			left.add(2);
			left.add(3);
			left.add(4);
			left.add(5);
			left.add(6);
			left.add(7);
			left.add(8);
			left.add(9);
			left.add(10);
			left.add(11);
			
			JSONArray things = o.getJSONArray("things");
			
			for(int i=0; i < things.length(); i++) {
				
				assertFalse((Integer)things.get(i) == 1);
				
				if(!left.contains(things.get(i)))
					fail();
				left.remove(things.get(i));
			}
			
			assertEquals(left.size(), 0);
			
		} catch (JSONException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testPop() {
		try {
			
			JSONObject o = new JSONObject("{things:[1,2,3,4,5,6,7,8,9,10,11]}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$pop':{things:0}}"), o);
			
			HashSet<Integer> left = new HashSet<Integer>();
			left.add(1);
			left.add(2);
			left.add(3);
			left.add(4);
			left.add(5);
			left.add(6);
			left.add(7);
			left.add(8);
			left.add(9);
			left.add(10);
			
			
			JSONArray things = o.getJSONArray("things");
			
			for(int i=0; i < things.length(); i++) {
				
				assertFalse((Integer)things.get(i) == 11);
				
				if(!left.contains(things.get(i)))
					fail();
				left.remove(things.get(i));
			}
			
			assertEquals(left.size(), 0);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testAddToSet() {
		try {
		
			// TODO: implement
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}	
	}
	
	@Test
	public void testPushAll() {
		try {
		
			JSONObject o = new JSONObject("{things:[0,1,2,3]}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$pushAll':{things:[4,5,6]}}"), o);
			
			JSONArray things = o.getJSONArray("things");
			for(int i=0; i < things.length(); i++) {
				assertEquals(things.get(i), i);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testPush() {
		try {
			JSONObject o = new JSONObject("{things:[0,1,2,3]}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$push':{things:4}}"), o);
			
			JSONArray things = o.getJSONArray("things");
			for(int i=0; i < things.length(); i++) {
				assertEquals(things.get(i), i);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testUnset() {
		try {
			
			JSONObject o = new JSONObject("{things:[0,1,2,3], extra:'b'}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$unset':{extra:1}}"), o);
			
			try {
				o.get("extra");
				fail();
			}
			catch (JSONException je) {
				
			}
			
			assertTrue(o.get("things") != null);
			
			// make sure things in unchanged
			JSONArray things = o.getJSONArray("things");
			for(int i=0; i < things.length(); i++) {
				assertEquals(things.get(i), i);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testSet() {
		try {
			JSONObject o = new JSONObject("{things:[0,1,2,3]}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$set':{extra:'b'}}"), o);
			
			
			String extra = o.getString("extra");
			
			
			assertEquals(extra, "b");
			
			JSONArray things = o.getJSONArray("things");
			for(int i=0; i < things.length(); i++) {
				assertEquals(things.get(i), i);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testIncrement() {
		try {
			JSONObject o = new JSONObject("{amount: 5}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$inc':{amount:12}}"), o);
			
			assertEquals(o.get("amount"), 17);
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$inc':{amount:-50}}"), o);
			
			assertEquals(o.get("amount"), -33);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void seriesTest() {
		try {
			JSONObject o = new JSONObject("{amount: 5}");
			
			JaccsonUpdater.applyUpdate(new JSONObject("{'$inc':{amount:12}}"), o);
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
