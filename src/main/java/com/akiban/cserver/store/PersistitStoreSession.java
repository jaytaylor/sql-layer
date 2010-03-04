package com.akiban.cserver.store;

import java.lang.ref.WeakReference;
import java.util.HashMap;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

public class PersistitStoreSession {

	private final Persistit db;
	
	private HashMap<Duple, WeakReference<Exchange>> cache = new HashMap<Duple, WeakReference<Exchange>>();

	private Exchange exchange1;

	private Exchange exchange2;

	private Exchange exchange3;

	private RowCollector currentRowCollector;

	private class Duple {
		final String volumeName;
		final String treeName;
		private Duple(final String volumeName, final String treeName) {
			this.volumeName= volumeName;
			this.treeName = treeName;
		}
		
		public boolean equals(final Object obj) {
			if (obj instanceof Duple) {
				final Duple d = (Duple)obj;
				return volumeName.equals(d.volumeName) && treeName.equals(d.treeName);
			}
			else {
				return false;
			}
		}
		
		public int hashCode() {
			return volumeName.hashCode() ^ treeName.hashCode();
		}
	}
	
	public PersistitStoreSession(final Persistit db) {
		this.db = db;
	}

	public Exchange getExchange(final String volumeName, final String treeName)
			throws PersistitException {
		final Duple d = new Duple(volumeName, treeName);
		WeakReference<Exchange> ref = cache.get(d);
		if (ref != null && ref.get() != null) {
			return ref.get();
		}
		final Exchange ex = db.getExchange(volumeName, treeName, true).clear();
		cache.put(d, new WeakReference<Exchange>(ex));
		return ex;
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
