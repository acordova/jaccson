package com.jaccson.mongo;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBPort;
import com.mongodb.WriteConcern;


public class WriteResult {

	WriteResult( CommandResult o , WriteConcern concern ){
		_lastErrorResult = o;
		_lastConcern = concern;
		_lazy = false;
		_port = null;
		_db = null;
	}

	WriteResult( DB db , DBPort p , WriteConcern concern ){
		_db = db;
		_port = p;
		//_lastCall = p._calls;
		_lastConcern = concern;
		_lazy = true;
	}

	/**
	 * Gets the last result from getLastError()
	 * @return
	 */
	public CommandResult getCachedLastError(){
		return _lastErrorResult;

	}

	/** 
	 * Gets the last {@link WriteConcern} used when calling getLastError()
	 * @return
	 */
	public WriteConcern getLastConcern(){
		return _lastConcern;

	}

	/**
	 * calls {@link WriteResult#getLastError(com.mongodb.WriteConcern)} with concern=null
	 * @return
	 */
	public synchronized CommandResult getLastError() {
		return getLastError(null);
	}

	/**
	 * This method does following:
	 * - returns the existing CommandResult if concern is null or less strict than the concern it was obtained with
	 * - otherwise attempts to obtain a CommandResult by calling getLastError with the concern
	 * @param concern the concern
	 * @return
	 */
	public synchronized CommandResult getLastError(WriteConcern concern) {
		if ( _lastErrorResult != null ) {
			// do we have a satisfying concern?
			if ( concern == null || ( _lastConcern != null && _lastConcern.getW() >= concern.getW() ) )
				return _lastErrorResult;
		}

		// here we dont have a satisfying result
		if ( _port != null ){
			/*try {
				 _lastErrorResult = _port.tryGetLastError( _db , _lastCall , (concern == null) ? new WriteConcern() : concern  );
			 } catch ( IOException ioe ){
				 throw new MongoException.Network( ioe.getMessage() , ioe );
			 }*/

			if (_lastErrorResult == null)
				throw new IllegalStateException( "The connection may have been used since this write, cannot obtain a result" );
			_lastConcern = concern;
			_lastCall++;
		} else {
			// this means we dont have satisfying result and cant get new one
			throw new IllegalStateException( "Don't have a port to obtain a write result, and existing one is not good enough." );
		}

		return _lastErrorResult;
	}


	/**
	 * Gets the error String ("err" field)
	 * @return
	 */
	public String getError() {
		Object foo = getField( "err" );
		if ( foo == null )
			return null;
		return foo.toString();
	}

	/**
	 * Gets the "n" field, which contains the number of documents
	 * affected in the write operation.
	 * @return
	 */
	public int getN(){
		return N;
	}

	/**
	 * Gets a field
	 * @param name field name
	 * @return
	 */
	public Object getField( String name ) {
		return getLastError().get( name );
	}

	/**
	 * Returns whether or not the result is lazy, meaning that getLastError was not called automatically
	 * @return
	 */
	public boolean isLazy() {
		return _lazy;
	}

	@Override
	public String toString(){
		CommandResult res = getCachedLastError();
		if (res != null)
			return res.toString();
		return "N/A";
	}

	private long _lastCall;
	private WriteConcern _lastConcern;
	private CommandResult _lastErrorResult;
	final private DB _db;
	final private DBPort _port;
	final private boolean _lazy;
	private int N;

	public void setN(int n) {
		N = n;

	}

}
