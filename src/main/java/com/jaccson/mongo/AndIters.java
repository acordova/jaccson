package com.jaccson.mongo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class AndIters implements Iterator<List<Range>> {

	private List<Iterator<Entry<Key,Value>>> iters;
	
	private String[] rows;
	private boolean spent = false;
	
	
	public AndIters(List<Iterator<Entry<Key,Value>>> iters) {
		
		this.iters = iters;
		rows = new String[iters.size()];
		
		int i=0;
		for(Iterator<Entry<Key,Value>> iter : iters) {
			
			if(!iter.hasNext()) {
				spent = true;
				break;
			}
			
			// load up first row of each iter
			rows[i] = iter.next().getKey().getColumnQualifier().toString();
			i++;
		}
	}
	
	
	private String nextRow() { 
				
		if(spent) {
			return null;
		}
		
		int i = 0;
		int j = 1;
		
		while(true) {
			
			if(i==j) {
				// advance one iterator to begin next round
				if(iters.get(0).hasNext()) {
					iters.get(0).next();
				}
				else {
					spent = true;
				}
				
				return rows[0];
			}
			
			int result = rows[i].compareTo(rows[j]); 
			if(result < 0) {
				if(iters.get(i).hasNext()) {
					rows[i] = iters.get(i).next().getKey().getColumnQualifier().toString();
				}
				else {
					spent = true;
					return null;
				}
			}
			else if(result > 0) {
				if(iters.get(j).hasNext()) {
					rows[j] = iters.get(j).next().getKey().getColumnQualifier().toString();
				}
				else {
					spent = true;
					return null;
				}
			}
			else {
				j = (j + 1) % rows.length;
			}
		}
		
	}

	public boolean hasNext() {
		
		return !spent;
	}


	public List<Range> next() {
		ArrayList<Range> rows = new ArrayList<Range>();
		
		//long start = System.currentTimeMillis();
		
		while(rows.size() < 100) { // && System.currentTimeMillis() - start < 1000L) {
			String next = nextRow();
			
			if(next == null)
				return rows;
			
			rows.add(new Range(next));
		}
		
		return rows;
	}

	
	public void remove() {}

}
