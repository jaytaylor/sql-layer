package com.akiba.cserver.store;

import java.util.HashMap;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

public class PersistitStoreSession {

	private final Persistit db;
	
	// TODO - currently ignoring volumeName.
	//
	private HashMap<String, Exchange> exchangeMap = new HashMap<String, Exchange>();

	private RowCollector currentRowCollector;
	
	public PersistitStoreSession(final Persistit db) {
		this.db = db;
	}
	
	public Exchange getExchange(final String volumeName, final String treeName)
			throws PersistitException {
		Exchange exchange = exchangeMap.get(treeName);
		if (exchange == null) {
			exchange = db.getExchange(volumeName, treeName, true);
			exchangeMap.put(treeName, exchange);
		}
		return exchange;
	}

	public RowCollector getCurrentRowCollector() {
		return currentRowCollector;
	}

	public void setCurrentRowCollector(RowCollector currentRowCollector) {
		this.currentRowCollector = currentRowCollector;
	}


}
