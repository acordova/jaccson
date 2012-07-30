package com.jaccson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Map;

import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import com.jaccson.server.JaccsonUpdater;

public class UpdaterTests {

	@Test
	public void testPullAll() {
		
		
			
			HashSet<Long> evens = new HashSet<Long>();
			evens.add(2L);
			evens.add(4L);
			evens.add(6L);
			evens.add(8L);
			evens.add(10L);
			
			HashSet<Long> odds = new HashSet<Long>();
			odds.add(1L);
			odds.add(3L);
			odds.add(5L);
			odds.add(7L);
			odds.add(9L);
			odds.add(11L);
			
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[1,2,3,4,5,6,7,8,9,10,11]}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$pullAll\":{\"things\":[2,4,6,8,10]}}")), o);
			
			BasicBSONList things = (BasicBSONList) o.get("things");
			for(int i=0; i <  things.size(); i++) {
				if(evens.contains(things.get(i))) {
					fail();
				}
			}
			
			for(int i=0; i <  things.size(); i++) {
				if(!odds.contains(things.get(i))) {
					fail();
				}
				odds.remove(things.get(i));
			}
			
			assertEquals(odds.size(), 0);
			
		
	}
	
	@Test
	public void testPull() {
		
		
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[1,2,3,4,5,6,7,8,9,10,11]}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$pull\":{\"things\":1}}")), o);
						
			HashSet<Long> left = new HashSet<Long>();
			left.add(2L);
			left.add(3L);
			left.add(4L);
			left.add(5L);
			left.add(6L);
			left.add(7L);
			left.add(8L);
			left.add(9L);
			left.add(10L);
			left.add(11L);
			
			BasicBSONList things = (BasicBSONList) o.get("things");
			
			for(int i=0; i < things.size(); i++) {
				
				assertFalse((Long)things.get(i) == 1);
				
				if(!left.contains(things.get(i)))
					fail();
				left.remove(things.get(i));
			}
			
			assertEquals(left.size(), 0);
			
		
	}

	@Test
	public void testPop() {
		try {
			
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[1,2,3,4,5,6,7,8,9,10,11]}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$pop\":{\"things\":0}}")), o);
			
			HashSet<Long> left = new HashSet<Long>();
			left.add(1L);
			left.add(2L);
			left.add(3L);
			left.add(4L);
			left.add(5L);
			left.add(6L);
			left.add(7L);
			left.add(8L);
			left.add(9L);
			left.add(10L);
			
			
			BasicBSONList things = (BasicBSONList) o.get("things");
			
			for(int i=0; i < things.size(); i++) {
				
				assertFalse((Long)things.get(i) == 11);
				
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
		
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[0,1,2,3]}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$pushAll\":{\"things\":[4,5,6]}}")), o);
			
			BasicBSONList things = (BasicBSONList) o.get("things");
			for(int i=0; i < things.size(); i++) {
				assertEquals(things.get(i), (long)i);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testPush() {
		try {
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[0,1,2,3]}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$push\":{\"things\":4}}")), o);
			
			BasicBSONList things = (BasicBSONList) o.get("things");
			for(int i=0; i < things.size(); i++) {
				assertEquals(things.get(i), (long)i);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testUnset() {
		try {
			
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[0,1,2,3], \"extra\":\"b\"}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$unset\":{\"extra\":1}}")), o);			
			
			assertTrue(o.get("things") != null);
			
			// make sure things in unchanged
			Object[] things = (Object[]) o.get("things");
			for(int i=0; i < things.length; i++) {
				assertEquals(things[i], (long)i);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testSet() {
		try {
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"things\":[0,1,2,3]}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$set\":{\"extra\":\"b\"}}")), o);
			
			
			String extra = o.getString("extra");
			
			
			assertEquals(extra, "b");
			
			Object[] things = (Object[]) o.get("things");
			for(int i=0; i < things.length; i++) {
				assertEquals(things[i], (long)i);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testIncrement() {
		try {
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"amount\": 5}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$inc\":{\"amount\":12}}")), o);
			
			assertEquals(o.get("amount"), 17L);
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$inc\":{\"amount\":-50}}")), o);
			
			assertEquals(o.get("amount"), -33L);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void seriesTest() {
		try {
			BasicBSONObject o = new BasicBSONObject((Map)JSON.parse("{\"amount\": 5}"));
			
			JaccsonUpdater.applyUpdate(new BasicBSONObject((Map)JSON.parse("{\"$inc\":{\"amount\":12}}")), o);
			
			assertEquals(o.get("amount"), 17L);
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
