package com.jaccson.server;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.log4j.Logger;

public class DeletedFilter extends Filter {

	static Logger log = Logger.getLogger(DeletedFilter.class);
	
	@Override
	public boolean accept(Key k, Value v) {
		log.info("deleter examining " + new String(v.get()));
		return !new String(v.get()).equals("{\"$delete\":1}");
	}
}
