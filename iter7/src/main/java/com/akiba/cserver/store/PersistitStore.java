package com.akiba.cserver.store;

import java.io.File;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Properties;

import com.akiba.ais.io.PersistitSource;
import com.akiba.ais.io.Reader;
import com.akiba.ais.io.Source;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.FieldDef;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;
import com.akiba.cserver.WriteRowResponse;
import com.akiba.message.AkibaConnection;
import com.akiba.message.Message;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.StreamLoader;
import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public class PersistitStore implements Store, CServerConstants {

	private final static String N = Persistit.NEW_LINE;
	
	private final static Properties PERSISTIT_PROPERTIES = new Properties();
	
	static {
		PERSISTIT_PROPERTIES.put("datapath", ".");
		PERSISTIT_PROPERTIES.put("logpath", "${datapath}");
		PERSISTIT_PROPERTIES.put("logfile","${logpath}/persistit_${timestamp}.log");
		PERSISTIT_PROPERTIES.put("verbose","true");
		PERSISTIT_PROPERTIES.put("buffer.count.8192","4K");
		PERSISTIT_PROPERTIES.put("volume.1","${datapath}/sys_txn.v0,create,pageSize:8K,initialSize:1M,extensionSize:1M,maximumSize:10G");
		PERSISTIT_PROPERTIES.put("volume.2","${datapath}/aktest.v01,create,pageSize:8k,initialSize:5M,extensionSize:5M,maximumSize:100G");
		PERSISTIT_PROPERTIES.put("pwjpath","${datapath}/persistit.pwj");
		PERSISTIT_PROPERTIES.put("pwjsize","8M");
		PERSISTIT_PROPERTIES.put("pwdelete","true");
		PERSISTIT_PROPERTIES.put("pwjcount","2");
		PERSISTIT_PROPERTIES.put("jmx","false");
		PERSISTIT_PROPERTIES.put("showgui", "false");
	}

	private static String datapath;

	private long startTime;

	private Persistit db;

	private ThreadLocal<HashMap<String, Exchange>> exchangeLocal = new ThreadLocal<HashMap<String, Exchange>>();

	private final RowDefCache rowDefCache;
	
	private AkibaInformationSchema ais;

	public static void setDataPath(final String path) {
		datapath = path;
	}

	public PersistitStore(final RowDefCache cache) {
		this.rowDefCache = cache;
	}

	public synchronized void startUp() throws Exception {

		// Util.printRuntimeInfo();

		if (db == null) {
			db = new Persistit();
			db.setProperty("datapath", datapath);
			final long t = System.currentTimeMillis();
			db.initialize(PERSISTIT_PROPERTIES);
			System.err.println("Persistit startup complete at: "
					+ db.elapsedTime() + "ms - took ("
					+ (System.currentTimeMillis() - t) + "ms)");
			System.err.flush();
			startTime = System.currentTimeMillis();
		}
	}

	public synchronized void shutDown() throws Exception {
		if (db != null) {
			final long t = System.currentTimeMillis();
			db.close();
			System.err.println("Persistit shutDown complete at: "
					+ db.elapsedTime() + "ms - took "
					+ (System.currentTimeMillis() - t) + "ms");
			System.err.println("Persitit was up for "
					+ (System.currentTimeMillis() - startTime) + "ms)");
			System.err.flush();
			db = null;
		}
	}

	public Persistit getDb() {
		return db;
	}

	public void loadAIS(final File fromFile) throws Exception {
		final StreamLoader loader = new StreamLoader(db, fromFile);
		final Exchange exchange = db.getExchange("aktest", "ais", true);
		loader.load(new StreamLoader.ImportHandler(db) {
			public void handleVolumeIdRecord(long volumeId, long initialPages,
					long extensionPages, long maximumPages, int bufferSize,
					String volumeName) throws PersistitException {

			}

			public void handleTreeIdRecord(int treeIndex, String treeName)
					throws PersistitException {
			}

			public void handleDataRecord(Key key, Value value)
					throws PersistitException {
				key.copyTo(exchange.getKey());
				value.copyTo(exchange.getValue());
				exchange.store();
			}
		});
		exchange.clear();
		final Source source = new PersistitSource(exchange);
		ais = new Reader(source).load();
	}

	// --------------------- Store interface --------------------

	public void writeRow(final AkibaConnection connection, final RowData rowData)
			throws Exception {
		final int result = writeRow(rowData);
		final Message message = new WriteRowResponse(result);
		connection.send(message);
	}

	// ----------------------------------------------------------

	int writeRow(final RowData rowData) {
		final int rowDefId = rowData.getRowDefId();
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		Transaction transaction = null;
		boolean done = false;
		try {
			final RowDef rootRowDef = rootRowDef(rowDef);
			final String treeName = rootRowDef.getTableName();
			final Exchange exchange = getExchange(treeName);
			transaction = db.getTransaction();
			transaction.begin();
			constructHKey(exchange.getKey(), rowDef, rowData);
			exchange.fetch();
			if (exchange.getValue().isDefined()) {
				throw new StoreException(NON_UNIQUE, "Non-unique key "
						+ exchange.getKey());
			}
			final int start = rowData.getInnerStart();
			final int size = rowData.getInnerSize();

			System.arraycopy(rowData.getBytes(), start, exchange.getValue()
					.getEncodedBytes(), 0, size);
			exchange.getValue().setEncodedSize(size);
			exchange.store();
			if (rowDef.getParentRowDefId() != 0) {
				//
				// For child rows we need to insert a pk index row
				//
				final Exchange pkExchange = getExchange(rowDef.getPkTreeName());
				copyAndRotate(exchange.getKey(), pkExchange.getKey(), -1);
				pkExchange.getValue().clear();
				pkExchange.store();
			}
			transaction.commit();
			done = true;
			return OK;

		} catch (StoreException e) {
			return e.getResult();
		} catch (Throwable t) {
			t.printStackTrace();
			return ERR;
		}

		finally {
			if (transaction != null) {
				if (!done) {
					try {
						transaction.rollback();
					} catch (RollbackException e) {
						// ignore so we can return the result code.
					} catch (PersistitException e) {
						e.printStackTrace();
					}
				}
				transaction.end();
			}
		}
	}

	private RowDef rootRowDef(final RowDef rowDef) throws StoreException {
		RowDef r = rowDef;
		while (r.getParentRowDefId() != 0) {
			r = rowDefCache.getRowDef(r.getParentRowDefId());
			if (r == null || r == rowDef) {
				throw new StoreException(MISSING_OR_CORRUPT_ROW_DEF,
						"Parent chain broken for " + rowDef);
			}
		}
		return r;
	}

	private Exchange getExchange(final String treeName)
			throws PersistitException {
		HashMap<String, Exchange> exchangeMap = exchangeLocal.get();
		if (exchangeMap == null) {
			exchangeMap = new HashMap<String, Exchange>();
			exchangeLocal.set(exchangeMap);
		}
		Exchange exchange = exchangeMap.get(treeName);
		if (exchange == null) {
			exchange = db.getExchange("aktest", treeName, true);
			exchangeMap.put(treeName, exchange);
		}
		return exchange;
	}

	private void constructHKey(final Key key, final RowDef rowDef,
			final RowData rowData) throws PersistitException, StoreException {
		key.clear();
		//
		// Constructing an h-key for a child table. We look at the
		// parent RowDef. If the parent is a root table, then the
		// child row must contain a foreign key, denoted by its
		// parentJoinFields. That's the root of our key.
		//
		// Otherwise, this child has a grandparent, we need to look
		// up the parent row using the parent row's PK index.
		//
		if (rowDef.getParentRowDefId() != 0) {
			final RowDef parentRowDef = rowDefCache.getRowDef(rowDef
					.getParentRowDefId());
			if (parentRowDef.getParentRowDefId() == 0) {
				//
				// No grandparent.
				//
				appendKeyFields(key, rowDef, rowData, rowDef
						.getParentJoinFields());
			} else {
				//
				// Yes grandparent. Use pkIndex lookup.
				//
				final Exchange indexExchange = getExchange(parentRowDef
						.getPkTreeName());
				final Key indexKey = indexExchange.getKey();
				indexKey.clear();
				appendKeyFields(indexKey, rowDef, rowData, rowDef
						.getParentJoinFields());
				if (!indexExchange.next(true)) {
					throw new StoreException(FOREIGN_KEY_MISSING, indexKey
							.toString());
				}
				copyAndRotate(indexKey, key, 1);
			}
		} else {
			key.clear();
		}
		//
		// Now append the primary key field(s) of the current row
		//
		appendKeyFields(key, rowDef, rowData, rowDef.getPkFields());
	}

	private void appendKeyFields(final Key key, final RowDef rowDef,
			final RowData rowData, final int[] fields)
			throws PersistitException {
		for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
			final FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
			final long location = rowDef.fieldLocation(rowData,
					fields[fieldIndex]);
			appendKeyField(key, fieldDef, rowData, location);
		}
	}

	private void appendKeyField(final Key key, final FieldDef fieldDef,
			final RowData rowData, final long location) {
		switch (fieldDef.getType()) {
		case TINYINT:
		case SMALLINT:
		case INT:
		case BIGINT:
			final long value = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(value);
			break;
		default:
			throw new UnsupportedOperationException(
					"Extend the key encoding logic to "
							+ "handle a key of type " + fieldDef);
		}
	}

	/**
	 * Copies key segments from one key to another. Places the last segment
	 * first and the shifts the remaining segments to the right.
	 * 
	 * @param fromKey
	 * @param toKey
	 */
	void copyAndRotate(final Key fromKey, final Key toKey, final int at) {
		final byte[] from = fromKey.getEncodedBytes();
		final byte[] to = toKey.getEncodedBytes();
		int size = fromKey.getEncodedSize();
		fromKey.indexTo(at);
		int k = fromKey.getIndex();
		System.arraycopy(from, k, to, 0, size - k);
		System.arraycopy(from, 0, to, size - k, k);
		toKey.setEncodedSize(size);
	}
}
