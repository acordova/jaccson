package com.jaccson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class RowIDBatchIter implements Iterator<List<Range>> {

	private Iterator<Entry<Key, Value>> iter;

	public RowIDBatchIter(Iterator<Entry<Key,Value>> i) {
		this.iter = i;
	}
	
	public boolean hasNext() {
		return iter.hasNext();
	}

	public List<Range> next() {

		ArrayList<Range> rows = new ArrayList<Range>();
		
		int i=0;
		while(iter.hasNext() && i < 100) {
			rows.add(new Range(iter.next().getKey().getColumnQualifier()));
		}
		
		return rows;
	}

	public void remove() {}

	
}
