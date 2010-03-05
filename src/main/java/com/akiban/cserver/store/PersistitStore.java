package com.akiban.cserver.store;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.MySQLErrorConstants;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.RowType;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;
import com.persistit.logging.ApacheCommonsLogAdapter;

public class PersistitStore implements CServerConstants, MySQLErrorConstants,
		Store {

	private static final Log LOG = LogFactory.getLog(PersistitStore.class
			.getName());

	private static final String P_DATAPATH = "cserver.datapath";
	
	private static final String VERBOSE_PROPERTY_NAME = "cserver.verbose";

	private static final String PERSISTIT_PROPERTY_PREFIX = "persistit.";
	
	static final int MAX_TRANSACTION_RETRY_COUNT = 10;

	final static String VOLUME_NAME = "aktest"; // TODO - select
	// database

	private final static Properties PERSISTIT_PROPERTIES = new Properties();

	static {
		PERSISTIT_PROPERTIES.put("logpath", "${datapath}");
		PERSISTIT_PROPERTIES.put("logfile",
				"${logpath}/persistit_${timestamp}.log");
		PERSISTIT_PROPERTIES.put("buffer.count.8192", "2K");
		PERSISTIT_PROPERTIES.put("volume.1",
				"${datapath}/sys_txn.v0,create,pageSize:8K,initialSize:1M,e"
						+ "xtensionSize:1M,maximumSize:10G");
		PERSISTIT_PROPERTIES.put("volume.2", "${datapath}/" + VOLUME_NAME
				+ ".v01,create,pageSize:8k,"
				+ "initialSize:5M,extensionSize:5M,maximumSize:100G");
		PERSISTIT_PROPERTIES.put("pwjpath", "${datapath}/persistit.pwj");
		PERSISTIT_PROPERTIES.put("pwjsize", "16M");
		PERSISTIT_PROPERTIES.put("pwdelete", "true");
		PERSISTIT_PROPERTIES.put("pwjcount", "2");
	}

	static String datapath = "/tmp/chunkserver_data";
	
	private boolean verbose = false;

	private CServerConfig config;

	private Persistit db;

	private final RowDefCache rowDefCache;

	private ThreadLocal<PersistitStoreSession> sessionLocal = new ThreadLocal<PersistitStoreSession>();

	public static void setDataPath(final String path) {
		datapath = path;
	}

	public PersistitStore(final CServerConfig config, final RowDefCache cache) {
		this.rowDefCache = cache;
		this.config = config;
	}

	public synchronized void startUp() throws Exception {

		// Util.printRuntimeInfo();

		if (db == null) {
			db = new Persistit();
			db.setPersistitLogger(new ApacheCommonsLogAdapter(LOG));
			//
			// This injects the "datapath" properties into the Persistit
			// properties; it is then referenced by substitution in other
			// Persistit properties.
			//
			final String path = config.property(P_DATAPATH, datapath);
			db.setProperty("datapath", path);

			if (LOG.isInfoEnabled()) {
				LOG.info("PersistitStore datapath=" + path);
			}
			for (final Map.Entry<Object, Object> entry : config.getProperties()
					.entrySet()) {
				final String key = (String) entry.getKey();
				final String value = (String) entry.getValue();
				if (key.startsWith(PERSISTIT_PROPERTY_PREFIX)) {
					db.setProperty(key.substring(PERSISTIT_PROPERTY_PREFIX
							.length()), value);
				}
			}

			db.initialize(PERSISTIT_PROPERTIES);
			db.getManagement().setDisplayFilter(
					new RowDataDisplayFilter(this, db.getManagement()
							.getDisplayFilter()));
			
			final String verboseString = config.property(VERBOSE_PROPERTY_NAME + "|false");
			if ("true".equalsIgnoreCase(verboseString)) {
				verbose = true;
			}
			
		}
	}

	public synchronized void shutDown() throws Exception {
		if (db != null) {
			db.close();
			db = null;
		}
	}

	public Persistit getDb() {
		return db;
	}

	PersistitStoreSession getSession() {
		PersistitStoreSession session = sessionLocal.get();
		if (session == null) {
			session = new PersistitStoreSession(db);
			sessionLocal.set(session);
		}
		return session;
	}

	Exchange getExchange(final RowDef rowDef, final IndexDef indexDef)
			throws PersistitException {
		final String treeName;
		if (indexDef == null) {
			final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef
					: rowDefCache.getRowDef(rowDef.getGroupRowDefId());
			treeName = groupRowDef.getTreeName();
		} else {
			treeName = indexDef.getTreeName();
		}
		return db.getExchange(VOLUME_NAME, treeName, true).clear();
	}

	void releaseExchange(final Exchange exchange) {
		if (exchange != null) {
			db.releaseExchange(exchange);
		}
	}

	/**
	 * Given a RowData for a table, construct an hkey for a row in the table.
	 * For a non-root table, this method uses the parent join columns as needed
	 * to find the hkey of the parent table. The attempt to look up the parent
	 * row may result in a StoreException due to a missing parent row; this is
	 * expressed as a HA_ERR_NO_REFERENCED_ROW error.
	 * 
	 * @param key
	 * @param rowData
	 */
	void constructHKey(final Exchange hEx, final RowDef rowDef,
			final RowData rowData) throws Exception {
		final Key hkey = hEx.getKey();
		hkey.clear();
		switch (rowDef.getRowType()) {
		case GROUP:
			throw new UnsupportedOperationException(
					"Cannot insert into a group table: "
							+ rowDef.getTableName());
		case ROOT:
			hkey.append(rowDef.getOrdinal());
			appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
			break;

		case CHILD: {
			final RowDef parentRowDef = rowDefCache.getRowDef(rowDef
					.getParentRowDefId());
			hkey.append(parentRowDef.getOrdinal());
			appendKeyFields(hkey, rowDef, rowData, rowDef.getParentJoinFields());
			if (!hEx.isValueDefined()) {
				throw new StoreException(HA_ERR_NO_REFERENCED_ROW, hkey
						.toString());
			}
			hkey.append(rowDef.getOrdinal());
			appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
			break;
		}

		case GRANDCHILD: {
			final RowDef parentRowDef = rowDefCache.getRowDef(rowDef
					.getParentRowDefId());
			final Exchange iEx = getExchange(rowDef, parentRowDef
					.getPKIndexDef());
			constructParentPKIndexKey(iEx.getKey(), rowDef, rowData);
			if (!iEx.hasChildren()) {
				throw new StoreException(HA_ERR_NO_REFERENCED_ROW, iEx.getKey()
						.toString());
			}
			boolean next = iEx.next(true);
			assert next;
			constructHKeyFromIndexKey(hkey, iEx.getKey(), parentRowDef
					.getPKIndexDef());
			hkey.append(rowDef.getOrdinal());
			appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
			releaseExchange(iEx);
		}
		}
	}

	/**
	 * Given a RowData, the hkey where it will be stored, and an IndexDef for a
	 * table, construct the index key.
	 * 
	 * @param rowData
	 * @param indexDef
	 */
	void constructIndexKey(final Key iKey, final RowData rowData,
			final IndexDef indexDef, final Key hKey) throws PersistitException {
		final IndexDef.H2I[] fassoc = indexDef.getIndexKeyFields();
		iKey.clear();
		for (int index = 0; index < fassoc.length; index++) {
			final IndexDef.H2I assoc = fassoc[index];
			if (assoc.getFieldIndex() >= 0) {
				final int fieldIndex = assoc.getFieldIndex();
				final RowDef rowDef = indexDef.getRowDef();
				final long location = rowDef.fieldLocation(rowData, fieldIndex);
				appendKeyField(iKey, rowDef.getFieldDef(fieldIndex), rowData,
						location);
			} else if (assoc.getHkeyLoc() >= 0) {
				appendKeyFieldFromKey(hKey, iKey, assoc.getHkeyLoc());
			} else {
				throw new IllegalStateException("Invalid FA");
			}
		}
	}

	/**
	 * Given an index key and an indexDef, construct the corresponding hkey for
	 * the row identified by the index key.
	 * 
	 * @param hkey
	 * @param indexKey
	 * @param indexDef
	 */
	void constructHKeyFromIndexKey(final Key hKey, final Key indexKey,
			final IndexDef indexDef) {
		final IndexDef.I2H[] fassoc = indexDef.getHkeyFields();
		hKey.clear();
		for (int index = 0; index < fassoc.length; index++) {
			final IndexDef.I2H fa = fassoc[index];
			if (fa.isOrdinalType()) {
				hKey.append(fa.getOrdinal());
			} else {
				final int depth = fassoc[index].getIndexKeyLoc();
				if (depth < 0 || depth > indexKey.getDepth()) {
					throw new IllegalStateException(
							"IndexKey too shallow - requires depth=" + depth
									+ ": " + indexKey);
				}
				appendKeyFieldFromKey(indexKey, hKey, fa.getIndexKeyLoc());
			}
		}
	}

	/**
	 * Given a RowData for a table, construct an Exchange set up with a Key that
	 * is the prefix of the parent's primary key index key.
	 * 
	 * @param rowData
	 */
	void constructParentPKIndexKey(final Key iKey, final RowDef rowDef,
			final RowData rowData) {
		iKey.clear();
		appendKeyFields(iKey, rowDef, rowData, rowDef.getParentJoinFields());
	}

	void appendKeyFields(final Key key, final RowDef rowDef,
			final RowData rowData, final int[] fields) {
		for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
			final FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
			final long location = rowDef.fieldLocation(rowData,
					fields[fieldIndex]);
			appendKeyField(key, fieldDef, rowData, location);
		}
	}

	void appendKeyField(final Key key, final FieldDef fieldDef,
			final RowData rowData, final long location) {
		switch (fieldDef.getType()) {
		case TINYINT:
		case SMALLINT:
		case INT:
		case BIGINT:
		case DATE:
		case DATETIME:
		case TIMESTAMP:
		case YEAR:
			final long value = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(value);
			break;
		case CHAR:
		case VARCHAR:
			final int start = (int) location;
			final int length = (int) (location >>> 32);
			// TODO: character encoding, collation
			key.append(new String(rowData.getBytes(), start, length));
			break;

		default:
			throw new UnsupportedOperationException(
					"Extend the key encoding logic to "
							+ "handle a key of type " + fieldDef);
		}
	}

	private void appendKeyFieldsFromHKey(final Key key, final int[] fields,
			final Key hKey) {
		for (int index = 0; index < fields.length; index++) {
			hKey.indexTo(fields[index]);
			int from = hKey.getIndex();
			hKey.indexTo(fields[index] + 1);
			int to = hKey.getIndex();
			if (from >= 0 && to >= 0 && to > from) {
				System.arraycopy(hKey.getEncodedBytes(), from, key
						.getEncodedBytes(), key.getEncodedSize(), to - from);
				key.setEncodedSize(key.getEncodedSize() + to - from);
			}
		}
	}

	private void appendKeyFieldFromKey(final Key fromKey, final Key toKey,
			final int depth) {
		fromKey.indexTo(depth);
		int from = fromKey.getIndex();
		fromKey.indexTo(depth + 1);
		int to = fromKey.getIndex();
		if (from >= 0 && to >= 0 && to > from) {
			System.arraycopy(fromKey.getEncodedBytes(), from, toKey
					.getEncodedBytes(), toKey.getEncodedSize(), to - from);
			toKey.setEncodedSize(toKey.getEncodedSize() + to - from);
		}

	}

	// --------------------- Implement Store interface --------------------

	@Override
	public RowDefCache getRowDefCache() {
		return rowDefCache;
	}
	
	@Override
	public boolean isVerbose() {
		return verbose;
	}

	@Override
	public int writeRow(final RowData rowData) {
		if (verbose && LOG.isInfoEnabled()) {
			LOG.info("Insert row: " + rowData.toString(rowDefCache));
		}

		final int rowDefId = rowData.getRowDefId();
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		Transaction transaction = null;
		Exchange hEx = null;
		try {
			hEx = getExchange(rowDef, null);

			transaction = db.getTransaction();
			int retries = MAX_TRANSACTION_RETRY_COUNT;
			for (;;) {
				transaction.begin();
				try {
					//
					// Does the heavy lifting of looking up the full hkey in
					// parent's primary index if necessary.
					//
					constructHKey(hEx, rowDef, rowData);

					if (hEx.isValueDefined()) {
						throw new StoreException(HA_ERR_FOUND_DUPP_KEY,
								"Non-unique key " + hEx.getKey());
					}

					final int start = rowData.getInnerStart();
					final int size = rowData.getInnerSize();
					hEx.getValue().ensureFit(size);
					System.arraycopy(rowData.getBytes(), start, hEx.getValue()
							.getEncodedBytes(), 0, size);
					hEx.getValue().setEncodedSize(size);

					// Store the h-row
					hEx.store();

					for (final IndexDef indexDef : rowDef.getIndexDefs()) {
						//
						// Insert the index keys (except for the case of a
						// root table's PK index.)
						//
						if (!indexDef.isHKeyEquivalent())
							insertIntoIndex(indexDef, rowDef, rowData, hEx
									.getKey());
					}
					transaction.commit();
					return OK;
				} catch (RollbackException re) {
					if (--retries < 0) {
						throw new TransactionFailedException();
					}
				} finally {
					transaction.end();
				}
			}
		} catch (StoreException e) {
			return e.getResult();
		} catch (Throwable t) {
			t.printStackTrace();
			return ERR;
		} finally {
			releaseExchange(hEx);
		}
	}

	@Override
	public int deleteRow(final RowData rowData) throws Exception {
		if (verbose && LOG.isInfoEnabled()) {
			LOG.info("Delete row: " + rowData.toString(rowDefCache));
		}
		final int rowDefId = rowData.getRowDefId();
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		Transaction transaction = null;
		Exchange hEx = null;
		;
		try {
			hEx = getExchange(rowDef, null);

			transaction = db.getTransaction();
			int retries = MAX_TRANSACTION_RETRY_COUNT;
			for (;;) {
				transaction.begin();
				try {
					constructHKey(hEx, rowDef, rowData);
					hEx.fetch();
					//
					// Verify that the row exists
					//
					if (!hEx.getValue().isDefined()) {
						throw new StoreException(HA_ERR_RECORD_DELETED,
								"Missing record at key " + hEx.getKey());
					}

					//
					// Verify that the row hasn't changed. Note: at some point
					// we
					// may want to optimize the protocol to send only PK and FK
					// fields in oldRowData, in which case this test will need
					// to change.
					//
					final int oldStart = rowData.getInnerStart();
					final int oldSize = rowData.getInnerSize();
					if (!bytesEqual(rowData.getBytes(), oldStart, oldSize, hEx
							.getValue().getEncodedBytes(), 0, hEx.getValue()
							.getEncodedSize())) {
						throw new StoreException(HA_ERR_RECORD_CHANGED,
								"Record changed at key " + hEx.getKey());
					}

					//
					// For Iteration 9 we disallow deleting rows that would
					// cascade to child rows.
					//
					if (hEx.hasChildren()) {
						throw new StoreException(UNSUPPORTED_MODIFICATION,
								"Can't cascade DELETE: " + hEx.getKey());

					}

					// Remove the h-row
					hEx.remove();

					// Remove the indexes, including the PK index
					for (final IndexDef indexDef : rowDef.getIndexDefs()) {
						if (!indexDef.isHKeyEquivalent()) {
							deleteIndex(indexDef, rowDef, rowData, hEx.getKey());
						}
					}
					transaction.commit();
					return OK;
				} catch (RollbackException re) {
					if (--retries < 0) {
						throw new TransactionFailedException();
					}
				} finally {
					transaction.end();
				}
			}
		} catch (StoreException e) {
			return e.getResult();
		} catch (Throwable t) {
			t.printStackTrace();
			return ERR;
		} finally {
			releaseExchange(hEx);
		}
	}

	@Override
	public int updateRow(final RowData oldRowData, final RowData newRowData) {
		if (LOG.isInfoEnabled()) {
			LOG.info("Update old: " + oldRowData.toString(rowDefCache));
			LOG.info("       new: " + oldRowData.toString(rowDefCache));
		}
		final int rowDefId = oldRowData.getRowDefId();
		if (newRowData.getRowDefId() != rowDefId) {
			throw new IllegalArgumentException(
					"RowData values have different rowDefId values: ("
							+ rowDefId + "," + newRowData.getRowDefId() + ")");
		}
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		Transaction transaction = null;
		Exchange hEx = null;
		try {
			hEx = getExchange(rowDef, null);

			transaction = db.getTransaction();
			int retries = MAX_TRANSACTION_RETRY_COUNT;
			for (;;) {
				transaction.begin();
				try {
					constructHKey(hEx, rowDef, oldRowData);
					hEx.fetch();
					//
					// Verify that the row exists
					//
					if (!hEx.getValue().isDefined()) {
						throw new StoreException(HA_ERR_RECORD_DELETED,
								"Missing record at key " + hEx.getKey());
					}
					//
					// Verify that it hasn't changed. Note: at some point we
					// may want to optimize the protocol to send only PK and FK
					// fields in oldRowData, in which case this test will need
					// to
					// change.
					//
					final int oldStart = oldRowData.getInnerStart();
					final int oldSize = oldRowData.getInnerSize();
					if (!bytesEqual(oldRowData.getBytes(), oldStart, oldSize,
							hEx.getValue().getEncodedBytes(), 0, hEx.getValue()
									.getEncodedSize())) {
						throw new StoreException(HA_ERR_RECORD_CHANGED,
								"Record changed at key " + hEx.getKey());
					}
					//
					// For Iteration 9, verify that only non-PK/FK fields are
					// changing - i.e., that the hkey will be the same.
					//
					final Key oldKey = hEx.getKey();
					final Key newKey = new Key(hEx.getKey());

					if (!fieldsEqual(rowDef, oldRowData, newRowData, rowDef
							.getPKIndexDef().getFields())) {
						throw new StoreException(UNSUPPORTED_MODIFICATION,
								"HKey change not supported: " + oldKey + "->"
										+ newKey);

					}

					final int start = newRowData.getInnerStart();
					final int size = newRowData.getInnerSize();
					hEx.getValue().ensureFit(size);
					System.arraycopy(newRowData.getBytes(), start, hEx
							.getValue().getEncodedBytes(), 0, size);
					hEx.getValue().setEncodedSize(size);

					// Store the h-row
					hEx.store();

					// Update the indexes
					//
					for (final IndexDef indexDef : rowDef.getIndexDefs()) {
						if (!indexDef.isHKeyEquivalent()) {
							updateIndex(indexDef, rowDef, oldRowData,
									newRowData, hEx.getKey());
						}
					}

					transaction.commit();
					return OK;
				} catch (RollbackException re) {
					if (--retries < 0) {
						throw new TransactionFailedException();
					}
				} finally {
					transaction.end();
				}
			}
		} catch (StoreException e) {
			return e.getResult();
		} catch (Throwable t) {
			t.printStackTrace();
			return ERR;
		} finally {
			releaseExchange(hEx);
		}
	}

	@Override
	public int dropTable(final int rowDefId) throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		if (verbose && LOG.isInfoEnabled()) {
			LOG.info("Drop table: " + rowDef.toString());
		}
		if (!rowDef.isGroupTable()) {
			throw new StoreException(HA_ERR_INTERNAL_ERROR,
					"Can't drop user tables yet");
		}
		final Transaction transaction = db.getTransaction();
		int retries = MAX_TRANSACTION_RETRY_COUNT;
		for (;;) {
			transaction.begin();
			try {
				//
				// Remove the index trees
				//
				for (IndexDef indexDef : rowDef.getIndexDefs()) {
					if (!indexDef.isHKeyEquivalent()) {
						final Exchange iEx = getExchange(rowDef, indexDef);
						iEx.getVolume().removeTree(iEx.getTree().getName());
						releaseExchange(iEx);
					}
				}
				//
				// remove the htable tree
				//
				final Exchange hEx = getExchange(rowDef, null);
				hEx.getVolume().removeTree(hEx.getTree().getName());
				releaseExchange(hEx);

				transaction.commit();
				return OK;
			} catch (RollbackException re) {
				if (--retries < 0) {
					throw new TransactionFailedException();
				}
			} finally {
				transaction.end();
			}
		}
	}

	@Override
	public long getAutoIncrementValue(final int rowDefId) throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		if (verbose && LOG.isInfoEnabled()) {
			LOG.info("Get auto-inc value for table: " + rowDef.toString());
		}
		final Exchange exchange;
		final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef : rowDefCache
				.getRowDef(rowDef.getGroupRowDefId());

		final String treeName = groupRowDef.getTreeName();

		switch (rowDef.getRowType()) {
		case GROUP:
			return -1L;
		case ROOT:
			exchange = db.getExchange(VOLUME_NAME, treeName, true);
			exchange.append(rowDef.getOrdinal());
			break;
		case CHILD:
		case GRANDCHILD:
			exchange = db
					.getExchange(VOLUME_NAME, rowDef.getPkTreeName(), true);
			break;
		default:
			throw new AssertionError("MissingCase");
		}
		exchange.getKey().append(Key.AFTER);
		boolean found = exchange.previous();
		long value = -1;
		if (found) {
			final Class<?> clazz = exchange.getKey().indexTo(-1).decodeType();
			if (clazz == Long.class) {
				value = exchange.getKey().decodeLong();
			}
		}
		releaseExchange(exchange);
		return value;
	}

	@Override
	public RowCollector getCurrentRowCollector() {
		return getSession().getCurrentRowCollector();
	}

	@Override
	public RowCollector newRowCollector(int indexId, RowData start,
			RowData end, byte[] columnBitMap) throws Exception {
		final int rowDefId = start.getRowDefId();
		if (end != null && end.getRowDefId() != rowDefId) {
			throw new IllegalArgumentException(
					"Start and end RowData must specify the same rowDefId");
		} 
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		
		if (verbose && LOG.isInfoEnabled()) {
			LOG.info("Select from table: " + rowDef.toString());
			LOG.info("  from: " + start.toString(rowDefCache));
			LOG.info("    to: " + end.toString(rowDefCache));
		}

		final RowCollector rc = new PersistitStoreRowCollector(this, start,
				end, columnBitMap, rowDef, indexId);

		getSession().setCurrentRowCollector(rc);
		return rc;
	}

	@Override
	public long getRowCount(final boolean exact, final RowData start,
			final RowData end, final byte[] columnBitMap) {
		//
		// TODO: Compute a reasonable value. The value "2" is a hack -
		// special because it's not 0 or 1, but small enough to induce
		// MySQL to use an index rather than full table scan.
		//
		return 2;
	}

	// ---------------------------------
	void insertIntoIndex(final IndexDef indexDef, final RowDef rowDef,
			final RowData rowData, final Key hkey) throws Exception {
		final Exchange iEx = getExchange(rowDef, indexDef);
		constructIndexKey(iEx.getKey(), rowData, indexDef, hkey);
		final Key key = iEx.getKey();

		if (indexDef.isUnique()) {
			int saveSize = key.getEncodedSize();
			key.setDepth(indexDef.getIndexKeySegmentCount());
			if (iEx.hasChildren()) {
				throw new StoreException(HA_ERR_FOUND_DUPP_KEY,
						"Non-unique index key: " + key.toString());
			}
			key.setEncodedSize(saveSize);
		}
		iEx.getValue().clear();
		iEx.store();
	}

	void updateIndex(final IndexDef indexDef, final RowDef rowDef,
			final RowData oldRowData, final RowData newRowData, final Key hkey)
			throws Exception {

		if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
			final Exchange oldExchange = getExchange(rowDef, indexDef);
			constructIndexKey(oldExchange.getKey(), oldRowData, indexDef, hkey);
			final Exchange newExchange = getExchange(rowDef, indexDef);
			constructIndexKey(newExchange.getKey(), newRowData, indexDef, hkey);

			oldExchange.getValue().clear();
			newExchange.getValue().clear();

			oldExchange.remove();
			newExchange.store();

			db.releaseExchange(newExchange);
			db.releaseExchange(oldExchange);
		}
	}

	void deleteIndex(final IndexDef indexDef, final RowDef rowDef,
			final RowData rowData, final Key hkey) throws Exception {
		final Exchange iEx = getExchange(rowDef, indexDef);
		constructIndexKey(iEx.getKey(), rowData, indexDef, hkey);
		iEx.remove();
		db.releaseExchange(iEx);
	}

	boolean bytesEqual(final byte[] a, final int aoffset, final int asize,
			final byte[] b, final int boffset, final int bsize) {
		if (asize != bsize) {
			return false;
		}
		for (int i = 0; i < asize; i++) {
			if (a[i + aoffset] != b[i + boffset]) {
				return false;
			}
		}
		return true;
	}

	boolean fieldsEqual(final RowDef rowDef, final RowData a, final RowData b,
			final int[] fieldIndexes) {
		for (int index = 0; index < fieldIndexes.length; index++) {
			final int fieldIndex = fieldIndexes[index];
			final long aloc = rowDef.fieldLocation(a, fieldIndex);
			final long bloc = rowDef.fieldLocation(b, fieldIndex);
			if (!bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32), b
					.getBytes(), (int) bloc, (int) (bloc >>> 32))) {
				return false;
			}
		}
		return true;
	}
}
