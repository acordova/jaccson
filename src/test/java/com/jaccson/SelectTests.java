package com.jaccson;


import junit.framework.TestCase;

import org.bson.BSONObject;

import com.jaccson.server.SelectIterator;
import com.mongodb.util.JSON;

/**
 * these test the SelectIterator functionality by itself, 
 * not deployed in the server and not through the client API
 * 
 * @author aaron
 *
 */
public class SelectTests extends TestCase {

	public void testSelectSimple() {

		BSONObject o = (BSONObject) JSON.parse("{\"a\":1, \"b\":2, \"c\":3}");

		BSONObject selected = SelectIterator.select(o, (BSONObject)JSON.parse("{\"a\":1}"));

		assertTrue(selected.toString().equals("{ \"a\" : 1}"));

	}

	public void testSelectTwo() {


		BSONObject o = (BSONObject) JSON.parse("{\"a\":1, \"b\":2, \"c\":3}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"a\":1, \"c\":1}"));

		assertTrue(selected.toString().equals("{ \"a\" : 1 , \"c\" : 3}") || selected.toString().equals("{ \"c\" : 3 , \"a\" : 1}"));


	}

	public void testSelectNested() {


		BSONObject o = (BSONObject) JSON.parse("{book:{author:\"bob\",\"price\":50}}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"book.author\":1}"));

		assertTrue(selected.toString().equals("{ \"book\" : { \"author\" : \"bob\"}}"));


	}

	public void testArray() {

		BSONObject o = (BSONObject) JSON.parse("{ \"books\" : [ \"a\" , \"b\" , \"c\"]}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"books\":1}"));

		assertTrue(selected.toString().equals("{ \"books\" : [ \"a\" , \"b\" , \"c\"]}"));

	}

	public void testArrayNested() {

		BSONObject o = (BSONObject) JSON.parse("{\"books\":[{\"author\":\"bob\",\"price\":50},{\"author\":\"george\",\"price\":40}]}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"books.author\":1}"));

		assertTrue(selected.toString().equals("{ \"books\" : [ { \"author\" : \"bob\" } , { \"author\" : \"george\"}]}"));

	}

	public void testTwoNested() {

		BSONObject o = (BSONObject) JSON.parse("{\"books\":[{\"author\":\"bob\",\"price\":50,\"copies\":12},{\"author\":\"george\",\"price\":40,\"copies\":4}]}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"books.author\":1, \"books.copies\":1}"));

		assertTrue(selected.toString().equals("{ \"books\" : [ { \"author\" : \"bob\" , \"copies\" : 12 } , { \"author\" : \"george\" , \"copies\" : 4}]}"));

	}

	public void testTwoAssymetric() {

		BSONObject o = (BSONObject) JSON.parse("{books:[{author:\"bob\",price:50,copies:12},{author:\"george\",price:40}]}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"books.author\":1, \"books.copies\":1}"));

		assertTrue(selected.toString().equals("{\"books\":[{\"author\":\"bob\",\"copies\":12},{\"author\":\"george\"}]}"));

	}

	public void testTwoTotallyAssymetric() {

		BSONObject o = (BSONObject) JSON.parse("{\"books:[{\"author\":\"bob\",\"price\":50,\"copies\":12},{\"writer\":\"george\",\"cost\":40}]}");

		BSONObject selected = SelectIterator.select(o, (BSONObject) JSON.parse("{\"books.author\":1, \"books.copies\":1}"));

		// mongo does this - leaves an empty object in an array
		assertTrue(selected.toString().equals("{\"books\":[{\"author\":\"bob\",\"copies\":12},{}]}"));
	}
}
