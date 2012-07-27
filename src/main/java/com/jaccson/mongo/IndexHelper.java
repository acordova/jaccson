package com.jaccson.mongo;

import java.nio.ByteBuffer;

public class IndexHelper {

	public static byte[] indexValueForObject(Object obj) {
		
		byte[] bytes = null;
		
		if (obj instanceof Double) {
			
			double d = (Double)obj;
			bytes = ByteBuffer.allocate(4).putDouble(d).array();
			
			bytes[0] ^= 0xff;
			
			if(d < 0) {
				bytes[1] ^= 0xff;
				bytes[2] ^= 0xff;
				bytes[3] ^= 0xff;
			}
		}
		
		else if (obj instanceof Integer) {
			
			// flip sign bit
			int i = (Integer)obj;
			
			i ^= 0x80000000;
			
			bytes = ByteBuffer.allocate(4).putInt(i).array();
		}
		
		else if (obj instanceof String) {
			
			bytes = ((String)obj).getBytes(); 
		}
		
		return bytes;
	}
	
	
}
