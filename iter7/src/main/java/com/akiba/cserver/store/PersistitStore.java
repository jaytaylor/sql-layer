package com.akiba.cserver.store;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

public class PersistitStore implements Store {

	private final RowDefCache rowDefCache = new RowDefCache();

	public final static int OK = 1;
	public final static int END = 2;
	public final static int ERR = 100;
	public final static int NON_UNIQUE = 101;

	private final static String N = Persistit.NEW_LINE;

	private final static String PROPERTIES = "datapath = ."
			+ N
			+ "logpath = ${datapath}"
			+ N
			+ "#rmiport = 1099"
			+ N
			+ "logfile = ${logpath}/persistit_${timestamp}.log"
			+ N
			+ "verbose = true"
			+ N
			+ "buffer.count.8192 = 32K"
			+ N
			+ "volume.1 = ${datapath}/sys_txn.v0,create,pageSize:8K,initialSize:1M,extensionSize:1M,maximumSize:10G"
			+ N
			+ "volume.2 = ${datapath}/aktest.v01,create,pageSize:8k,initialSize:5M,extensionSize:5M,maximumSize:100G"
			+ N + "pwjpath  = ${datapath}/persistit.pwj" + N + "pwjsize  = 8M"
			+ N + "pwjdelete = true" + N + "pwjcount = 2" + N + "jmx = false"
			+ N + "showgui = false" + N;

	private static String datapath;

	private static Persistit db;

	private static long startTime;

	private static ThreadLocal<HashMap<String, Exchange>> exchangeLocal = new ThreadLocal<HashMap<String, Exchange>>();

	public static void setDataPath(final String path) {
		datapath = path;
	}

	public synchronized void startUp() throws Exception {

		// Util.printRuntimeInfo();

		if (db == null) {
			final Properties properties = new Properties();
			properties.load(new StringReader(PROPERTIES));
			db = new Persistit();
			db.setProperty("datapath", datapath);
			final long t = System.currentTimeMillis();
			db.initialize(properties);
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

	// --------------------- Store interface --------------------

	public void writeRow(final AkibaConnection connection, final RowData rowData)
			throws Exception {
		final int result = writeRow(rowData);
		final Message message = new WriteRowResponse(result);
		connection.send(message);
	}

	// ----------------------------------------------------------

	private int writeRow(final RowData rowData) {
		final int rowDefId = rowData.getRowDefId();
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		final String treeName = rowDef.getTableName();
		Transaction transaction = null;
		try {
			final Exchange exchange = getExchange(treeName);
			transaction = db.getTransaction();
			transaction.begin();
			constructHKey(exchange.getKey(), rowDef, rowData);
			exchange.fetch();
			if (exchange.getValue().isDefined()) {
				return NON_UNIQUE;
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
				final Exchange pkExchange = getExchange(pkTreeName(rowDef));
				invertKey(exchange.getKey(), pkExchange.getKey());
				pkExchange.getValue().clear();
				pkExchange.store();
			}
			transaction.commit();
			return OK;
		} catch (Throwable t) {
			t.printStackTrace();
			return ERR;
		}

		finally {
			if (transaction != null) {
				transaction.end();
			}
		}
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
			final RowData rowData) throws PersistitException {
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
				// No grandparent
				//
				appendKeyFields(key, rowDef, rowData, rowDef
						.getParentJoinFields());
			} else {
				//
				// Yes grandparent. We need to fetch the parent row and
				// recursively
				// look from there.
				//
				final RowData parentRow = lookupRow(parentRowDef, rowDef,
						rowData);
				constructHKey(key, parentRowDef, parentRow);
			}
		} else {
			key.clear();
		}
		//
		// Now append the primary key field(s) of the current row
		//
		appendKeyFields(key, rowDef, rowData, rowDef.getPkFields());
	}

	private RowData lookupRow(final RowDef rowDef, final RowDef childRowDef,
			final RowData childRowData) throws PersistitException {
		if (rowDef.getParentRowDefId() == 0) {
			throw new IllegalArgumentException("RowDef must have a parent");
		}
		final String treeName = pkTreeName(rowDef);
		final Exchange exchange = getExchange(treeName);
		final Key key = exchange.getKey();
		key.clear();
		appendKeyFields(key, childRowDef, childRowData, childRowDef
				.getParentJoinFields());
		exchange.traverse(Key.GT, true);

		// TODO - unfinished...

		return null;
	}

	private void appendKeyFields(final Key key, final RowDef rowDef,
			final RowData rowData, final int[] fields)
			throws PersistitException {
		for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
			final FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
			final long location = rowDef.fieldLocation(rowData, fieldIndex);
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

	private String pkTreeName(final RowDef rowDef) {
		return rowDef.getTableName() + "_pk";
	}

	/**
	 * Copies key segments from one key to another. Places the last segment
	 * first and the shifts the remaining segments to the right.
	 * 
	 * @param fromKey
	 * @param toKey
	 */
	private void invertKey(final Key fromKey, final Key toKey) {
		final byte[] from = fromKey.getEncodedBytes();
		final byte[] to = toKey.getEncodedBytes();
		int size = fromKey.getEncodedSize();
		int k = 0;
		while (k < size && from[k++] != 0) {
		}
		System.arraycopy(from, k, to, 0, size - k);
		System.arraycopy(from, 0, to, size - k, k);
	}
}
