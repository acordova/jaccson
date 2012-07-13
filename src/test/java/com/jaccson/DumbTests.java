package com.jaccson;

import org.json.JSONException;
import org.json.JSONObject;

public class DumbTests {
	
	public static void main(String[] args) {
		try {
			JSONObject o = new JSONObject("{$delete:1}");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
