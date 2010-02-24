package com.akiban.cserver.store;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

public class PersistitStoreSession {

	private final Persistit db;

	private Exchange exchange1;

	private Exchange exchange2;

	private Exchange exchange3;

	private RowCollector currentRowCollector;

	public PersistitStoreSession(final Persistit db) {
		this.db = db;
	}

	public Exchange getExchange(final String volumeName, final String treeName)
			throws PersistitException {
		return db.getExchange(volumeName, treeName, true).clear();
	}

	public void releaseExchange(final Exchange exchange) {
		db.releaseExchange(exchange);
	}

	public Exchange getExchange1(final String volumeName, final String treeName)
			throws PersistitException {
		if (exchange1 == null
				|| !exchange1.getTree().getName().equals(treeName)) {
			exchange1 = db.getExchange(volumeName, treeName, true);
		}
		return exchange1.clear();
	}

	public Exchange getExchange2(final String volumeName, final String treeName)
			throws PersistitException {
		if (exchange2 == null
				|| !exchange2.getTree().getName().equals(treeName)) {
			exchange2 = db.getExchange(volumeName, treeName, true);
		}
		return exchange2.clear();
	}

	public Exchange getExchange3(final String volumeName, final String treeName)
			throws PersistitException {
		if (exchange3 == null
				|| !exchange3.getTree().getName().equals(treeName)) {
			exchange3 = db.getExchange(volumeName, treeName, true);
		}
		return exchange3.clear();
	}

	public RowCollector getCurrentRowCollector() {
		return currentRowCollector;
	}

	public void setCurrentRowCollector(RowCollector currentRowCollector) {
		this.currentRowCollector = currentRowCollector;
	}

	public void close() {
		if (exchange1 != null) {
			db.releaseExchange(exchange1);
			exchange1 = null;
		}
		if (exchange2 != null) {
			db.releaseExchange(exchange2);
			exchange2 = null;
		}
		if (exchange3 != null) {
			db.releaseExchange(exchange3);
			exchange3 = null;
		}
	}
}
