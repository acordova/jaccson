package com.jaccson;

import org.json.JSONException;
import org.json.JSONObject;

import junit.framework.TestCase;

public class SelectTests extends TestCase {

	public void testSelectSimple() {

		try {
			JSONObject o = new JSONObject("{a:1, b:2, c:3}");

			JSONObject selected = SelectFilter.select(o, new JSONObject("{a:1}"));

			assertTrue(selected.toString().equals("{\"a\":1}"));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		}
	}

	public void testSelectTwo() {

		try {
			JSONObject o = new JSONObject("{a:1, b:2, c:3}");

			JSONObject selected = SelectFilter.select(o, new JSONObject("{a:1, c:1}"));

			assertTrue(selected.toString().equals("{\"a\":1,\"c\":3}") || selected.toString().equals("{\"c\":3,\"a\":1}"));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testSelectNested() {

		try {
			JSONObject o = new JSONObject("{book:{author:'bob',price:50}}");

			JSONObject selected = SelectFilter.select(o, new JSONObject("{book.author:1}"));

			assertTrue(selected.toString().equals("{\"book\":{\"author\":\"bob\"}}"));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testArray() {
		try {
			JSONObject o = new JSONObject("{books:['a','b','c']}");

			JSONObject selected = SelectFilter.select(o, new JSONObject("{books:1}"));

			assertTrue(selected.toString().equals("{\"books\":[\"a\",\"b\",\"c\"]}"));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testArrayNested() {
		try {
			JSONObject o = new JSONObject("{books:[{author:'bob',price:50},{author:'george',price:40}]}");

			JSONObject selected = SelectFilter.select(o, new JSONObject("{books.author:1}"));

			assertTrue(selected.toString().equals("{\"books\":[{\"author\":\"bob\"},{\"author\":\"george\"}]}"));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testTwoNested() {
		try {
			JSONObject o = new JSONObject("{books:[{author:'bob',price:50,copies:12},{author:'george',price:40,copies:4}]}");

			JSONObject selected = SelectFilter.select(o, new JSONObject("{books.author:1, books.copies:1}"));

			assertTrue(selected.toString().equals("{\"books\":[{\"author\":\"bob\",\"copies\":12},{\"author\":\"george\",\"copies\":4}]}"));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue(false);
		}
	}
}
