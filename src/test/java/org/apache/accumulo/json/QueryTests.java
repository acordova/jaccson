package org.apache.accumulo.json;

import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.jaccson.JaccsonConnection;
import com.jaccson.JaccsonTable;

public class QueryTests {

	@Test
	public void testIn() {
		JaccsonConnection conn;
		Random r = new Random();

		try {
			conn = new JaccsonConnection("localhost", "acc", "root", "secret", "");

			conn.dropTable("intest");
			JaccsonTable table = conn.getTable("intest");
			table.ensureIndex(new JSONObject("{wheel_bin:1}"));

			for(int i=0; i < 20; i++) { 
				String bins = "[";
				for(int j=0; j < 3; j++) 
					bins += "'" + Integer.toString(Math.abs(r.nextInt()) % 20) + "',";
				bins = bins.substring(0, bins.length()-1);
				table.insert("{wheel_bin:" + bins + "]}");
			}
			
			for(JSONObject o : table.find("{'wheel_bin': {'$in': ['19','4','2','34']}}", "")) {
				System.out.println(o);
			}
			
			conn.dropTable("intest");

		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		}
	}
}
