package com.akiban.cserver.store;

public class StoreException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private final int result;
	
	public StoreException(final int result, final String message) {
		super(message);
		this.result = result;
	}
	
	public int getResult() {
		return result;
	}

}
