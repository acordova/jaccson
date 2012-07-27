package com.jaccson.server;

import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BSONObject;

import com.mongodb.util.JSON;

public class DeletedFilter extends Filter {

	static Logger log = Logger.getLogger(DeletedFilter.class);
	
	public static final byte[] DELETE_MARKER = BSON.encode((BSONObject) JSON.parse("{'$delete':1}"));
	public static final Value DELETED_VALUE = new Value(DELETE_MARKER);
	
	@Override
	public boolean accept(Key k, Value v) {
		log.info("deleter examining " + BSON.decode(v.get()).toString());
		return !Arrays.equals(DELETE_MARKER, v.get());
	}
}
