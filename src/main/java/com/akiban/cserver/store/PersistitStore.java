package com.akiban.cserver.store;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.io.Reader;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Source;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.StreamLoader;
import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;
import com.persistit.logging.ApacheCommonsLogAdapter;

public class PersistitStore implements Store, CServerConstants {

	private static final Log LOG = LogFactory.getLog(PersistitStore.class
			.getName());

	private static final String P_DATAPATH = "cserver.datapath";

	private static final String PERSISTIT_PROPERTY_PREFIX = "persistit.";

	static final int MAX_RETRY_COUNT = 10;

	final static String VOLUME_NAME = "aktest"; // TODO - select
	// database

	private final static Properties PERSISTIT_PROPERTIES = new Properties();

	static {
		PERSISTIT_PROPERTIES.put("logpath", "${datapath}");
		PERSISTIT_PROPERTIES.put("logfile",
				"${logpath}/persistit_${timestamp}.log");
		PERSISTIT_PROPERTIES.put("buffer.count.8192", "4K");
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

	private CServerConfig config;

	private Persistit db;

	private ThreadLocal<PersistitStoreSession> sessionLocal = new ThreadLocal<PersistitStoreSession>();

	private final RowDefCache rowDefCache;

	private AkibaInformationSchema ais;

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
			db.setProperty("datapath", config.property(P_DATAPATH, datapath));
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

	// --------------------- Store interface --------------------

	@Override
	public int writeRow(final RowData rowData) {
		final int rowDefId = rowData.getRowDefId();
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		Transaction transaction = null;
		try {
			final RowDef rootRowDef = rootRowDef(rowDef);
			final String treeName = rootRowDef.getTreeName();
			final Exchange exchange = getSession().getExchange1(VOLUME_NAME,
					treeName);
			transaction = db.getTransaction();
			int retries = MAX_RETRY_COUNT;
			for (;;) {
				transaction.begin();
				try {
					constructHKey(exchange.getKey(), rowDef, rowData);
					exchange.fetch();
					if (exchange.getValue().isDefined()) {
						throw new StoreException(HA_ERR_FOUND_DUPP_KEY,
								"Non-unique key " + exchange.getKey());
					}
					final int start = rowData.getInnerStart();
					final int size = rowData.getInnerSize();
					exchange.getValue().ensureFit(size);
					System.arraycopy(rowData.getBytes(), start, exchange
							.getValue().getEncodedBytes(), 0, size);
					exchange.getValue().setEncodedSize(size);

					// Store the h-row
					exchange.store();

					if (rowDef.getParentRowDefId() != 0) {
						//
						// For child rows we need to insert a pk index row
						//
						final Exchange pkExchange = getSession().getExchange3(
								VOLUME_NAME, rowDef.getPkTreeName());
						copyAndRotate(exchange.getKey(), pkExchange.getKey(),
								-(rowDef.getPkFields().length + 1));
						pkExchange.getValue().clear();
						pkExchange.store();
					}
					for (final IndexDef indexDef : rowDef.getIndexDefs()) {
						insertIntoIndex(indexDef, rowDef, rowData, exchange
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
		}
	}

	void insertIntoIndex(final IndexDef indexDef, final RowDef rowDef,
			final RowData rowData, final Key hkey) throws Exception {
		final Exchange exchange = getSession().getExchange(VOLUME_NAME,
				indexDef.getTreeName());
		final Key key = exchange.getKey();
		exchange.getValue().clear();
		for (int index = 0; index < indexDef.getFields().length; index++) {
			final int fieldIndex = indexDef.getFields()[index];
			final FieldDef fieldDef = rowDef.getFieldDef(fieldIndex);
			final long location = rowDef.fieldLocation(rowData, fieldIndex);
			appendKeyField(key, fieldDef, rowData, location);
		}
		if (indexDef.isUnique()) {
			int saveSize = key.getEncodedSize();
			key.append(Key.BEFORE);
			if (exchange.next()) {
				throw new StoreException(HA_ERR_FOUND_DUPP_KEY,
						"Non-unique index key: " + exchange.getKey().toString());
			}
			key.setEncodedSize(saveSize);
		}
		System.arraycopy(hkey.getEncodedBytes(), 0, key.getEncodedBytes(), key
				.getEncodedSize(), hkey.getEncodedSize());
		key.setEncodedSize(key.getEncodedSize() + hkey.getEncodedSize());
		exchange.store();
	}

	@Override
	public int dropTable(final int rowDefId) throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		if (!rowDef.isGroupTable()) {
			throw new StoreException(HA_ERR_INTERNAL_ERROR,
					"Can't drop user tables yet");
		}
		final Transaction transaction = db.getTransaction();
		int retries = MAX_RETRY_COUNT;
		for (;;) {
			transaction.begin();
			try {
				final Volume volume = db.getVolume(VOLUME_NAME);
				// Remove the index trees
				for (int index = 0; index < rowDef.getUserRowDefIds().length; index++) {
					final RowDef userRowDef = rowDefCache.getRowDef(rowDef
							.getUserRowDefIds()[index]);
					if (userRowDef.getParentRowDefId() != 0) {
						volume.removeTree(userRowDef.getPkTreeName());
					}
				}
				// remove the htable tree
				volume.removeTree(rowDef.getTreeName());
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

	public long getAutoIncrementValue(final int rowDefId) throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		final Exchange exchange;
		if (rowDef.getParentRowDefId() != 0) {
			exchange = getSession().getExchange3(VOLUME_NAME,
					rowDef.getPkTreeName());
		} else {
			exchange = getSession().getExchange1(VOLUME_NAME,
					rootRowDef(rowDef).getTreeName());
		}
		exchange.getKey().clear().append(rowDefId % MAX_VERSIONS_PER_TABLE)
				.append(Key.AFTER);
		boolean found = exchange.previous();
		long value = -1;
		if (found) {
			final Class<?> clazz = exchange.getKey().indexTo(-1).decodeType();
			if (clazz == Long.class) {
				value = exchange.getKey().decodeLong();
			}
		}
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

		final RowCollector rc = new PersistitStoreRowCollector(this, start,
				end, columnBitMap, rowDef);

		getSession().setCurrentRowCollector(rc);
		return rc;
	}

	@Override
	public long getRowCount(final boolean exact, final RowData start,
			final RowData end, final byte[] columnBitMap) {
		return 10000; // TODO: temporary hack
	}

	// ---------------------
	private RowDef rootRowDef(final RowDef rowDef) throws StoreException {
		RowDef root = rowDef;
		int depth = 0;
		while (root.getParentRowDefId() != 0) {
			root = rowDefCache.getRowDef(root.getParentRowDefId());
			if (root == null || root == rowDef || depth++ > MAX_GROUP_DEPTH) {
				throw new StoreException(MISSING_OR_CORRUPT_ROW_DEF,
						"Parent chain broken for " + rowDef);
			}
		}
		return root;
	}

	PersistitStoreSession getSession() {
		PersistitStoreSession session = sessionLocal.get();
		if (session == null) {
			session = new PersistitStoreSession(db);
			sessionLocal.set(session);
		}
		return session;
	}

	RowDefCache getRowDefCache() {
		return rowDefCache;
	}

	void constructHKey(final Key key, final RowDef rowDef, final RowData rowData)
			throws PersistitException, StoreException {
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
						.getParentJoinFields(), parentRowDef.getRowDefId());
			} else {
				//
				// Yes grandparent. Use pkIndex lookup.
				//
				final Exchange indexExchange = getSession().getExchange3(
						VOLUME_NAME, parentRowDef.getPkTreeName());
				final Key indexKey = indexExchange.getKey();
				indexKey.clear();
				appendKeyFields(indexKey, rowDef, rowData, rowDef
						.getParentJoinFields(), parentRowDef.getRowDefId());
				if (!indexExchange.next(true)) {
					throw new StoreException(HA_ERR_NO_REFERENCED_ROW, indexKey
							.toString());
				}
				copyAndRotate(indexKey, key, rowDef.getPkFields().length + 1);
			}
		} else {
			key.clear();
		}
		//
		// Now append the primary key field(s) of the current row
		//
		appendKeyFields(key, rowDef, rowData, rowDef.getPkFields(), rowDef
				.getRowDefId());
	}

	void appendKeyFields(final Key key, final RowDef rowDef,
			final RowData rowData, final int[] fields, final int rowDefId)
			throws PersistitException {
		key.append(rowDefId % MAX_VERSIONS_PER_TABLE);
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

	/**
	 * Return a value extracted from a RowDef as an object suitable for
	 * inclusion in a KeyFilter.RangeTerm. This method parallels
	 * {@link #appendKeyField(Key, FieldDef, RowData, long)} and at some point
	 * should be merged so that there is only one translation code path.
	 * 
	 * @param fieldDef
	 * @param rowData
	 * @param location
	 * @return Field value as Object
	 */
	Object keyField(final FieldDef fieldDef, final RowData rowData,
			final long location) {
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
			return Long.valueOf(value);

		case CHAR:
		case VARCHAR:
			final int start = (int) location;
			final int length = (int) (location >>> 32);
			// TODO: character encoding, collation
			return new String(rowData.getBytes(), start, length);

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

	void logRowData(final String prefix, final RowData rowData) {
		if (LOG.isInfoEnabled()) { // TODO - change to trace
			rowData.prepareRow(rowData.getRowStart());
			LOG.info(prefix + rowData.toString(rowDefCache));
		}
	}
}
