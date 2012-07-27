package com.jaccson.mongo;

/**
 * just a place to configure the ordering of iterators
 * 
 * @author aaron
 *
 */
public class IterStack {

	public final static int UPDATER_ITERATOR_PRI = 10;
	public final static int DELETED_ITERATOR_PRI = 11;
	public static final int FILTER_ITERATOR_PRI = 12;
	public final static int SELECT_ITERATOR_PRI = 13;
	
}
