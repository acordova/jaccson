package org.apache.accumulo.json;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jaccson.JSONHelper;

import junit.framework.TestCase;

public class JSONTests extends TestCase {

	public void testSubObject() {
		
		JSONObject o;
		try {
			o = new JSONObject("{title:'the title', author:{first:'bob', last:'dobbs'}, copyright:2011}");
			
			System.out.println(JSONHelper.subObjectForPath("title", o));
			System.out.println(JSONHelper.subObjectForPath("author", o));
			System.out.println(JSONHelper.subObjectForPath("author.first", o));
			
			
		} catch (JSONException e) {
		
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testSubWithArrays() {
		
		try {
			JSONObject o = new JSONObject("{author:{first:'bob', last:'dobbs'}, books:[{title:'subgenius handbook', price:400},{title:'slack', price:200}]}");
			System.out.println(JSONHelper.subObjectForPath("author", o));
			System.out.println(JSONHelper.subObjectForPath("books.price", o));
			System.out.println(JSONHelper.subObjectForPath("books", o));
			
			JSONArray arr = new JSONArray();
			arr.put(new JSONObject("{title:'sdf',price:2}"));
			arr.put(new JSONObject("{title:'aaa',price:4}"));
			
			System.out.println(JSONHelper.subObjectForPath("price", arr));
			
			
			JSONObject o2 = new JSONObject("{airport:'jfk', airlines:[[{name:'american',flights:20,passengers:[{name:'bob',age:20},{name:'jill', age:22}]},{name:'united',flights:10}],[{name:'jetblue',flights:2}]]}");
			System.out.println(JSONHelper.subObjectForPath("airlines.passengers.age", o2));
			
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}
		
	}
	
	public void testValuesForPath() {
		
		try {
			JSONObject o = new JSONObject("{memberID: '123', author:{first:'bob', last:'dobbs'}, books:[{title:'subgenius handbook', price:{dollars:400, euros:300}},{title:'slack', price:{dollars:200, euros:180}}]}");
			
			// simple, one level
			assertEquals(JSONHelper.valueForPath("memberID", o).toString(), "123");
			
			// simple, two levels
			assertEquals(JSONHelper.valueForPath("author.last", o).toString(), "dobbs");
			
			// multiple, last level
			Object values = JSONHelper.valueForPath("books.title", o);
			if(values instanceof List) {
				for(Object v : (List)values) {
					System.out.println(v);
				}
			}
			
			// multiple, mid-level
			values = JSONHelper.valueForPath("books.price.dollars", o);
			if(values instanceof List) {
				for(Object v : (List)values)
					System.out.println(v);
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
}
