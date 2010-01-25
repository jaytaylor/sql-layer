package com.akiba.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiba.ais.io.PersistitSource;
import com.akiba.ais.io.Reader;
import com.akiba.ais.io.Source;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.cserver.CServer;
import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.FieldDef;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;
import com.akiba.cserver.Util;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.Persistit;
import com.persistit.StreamLoader;
import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.Management.DisplayFilter;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.logging.ApacheCommonsLogAdapter;

public class PersistitStore implements Store, CServerConstants {

	private static final Log LOG = LogFactory.getLog(CServer.class.getName());

	private final static String VOLUME_NAME = "aktest"; // TODO - select
	// database

	private final static Properties PERSISTIT_PROPERTIES = new Properties();

	static {
		PERSISTIT_PROPERTIES.put("datapath", "/tmp/chunkserver_data");
		PERSISTIT_PROPERTIES.put("logpath", "${datapath}");
		PERSISTIT_PROPERTIES.put("logfile",
				"${logpath}/persistit_${timestamp}.log");
		PERSISTIT_PROPERTIES.put("verbose", "true");
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

	private static String datapath;

	private Persistit db;

	private ThreadLocal<PersistitStoreSession> sessionLocal = new ThreadLocal<PersistitStoreSession>();

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
			db.setPersistitLogger(new ApacheCommonsLogAdapter(LOG));
			db.setProperty("datapath", datapath);
			db.initialize(PERSISTIT_PROPERTIES);
			db.getManagement().setDisplayFilter(
					new RowDataDisplayFilter(db.getManagement()
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
		rowDefCache.setAIS(ais);
	}

	// --------------------- Store interface --------------------

	public int writeRow(final RowData rowData) {
		final int rowDefId = rowData.getRowDefId();
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		Transaction transaction = null;
		boolean done = false;
		try {
			final RowDef rootRowDef = rootRowDef(rowDef);
			final String treeName = rootRowDef.getTreeName();
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
			exchange.getValue().ensureFit(size);
			System.arraycopy(rowData.getBytes(), start, exchange.getValue()
					.getEncodedBytes(), 0, size);
			exchange.getValue().setEncodedSize(size);
			exchange.store();
			if (rowDef.getParentRowDefId() != 0) {
				//
				// For child rows we need to insert a pk index row
				//
				final Exchange pkExchange = getExchange(rowDef.getPkTreeName());
				copyAndRotate(exchange.getKey(), pkExchange.getKey(), -(rowDef
						.getPkFields().length + 1));
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

	public long getAutoIncrementValue(final int rowDefId) throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		final Exchange exchange;
		if (rowDef.getParentRowDefId() != 0) {
			exchange = getExchange(rowDef.getPkTreeName());
		} else {
			exchange = getExchange(rootRowDef(rowDef).getTreeName());
		}
		exchange.getKey().clear().append(rowDefId % MAX_VERSIONS_PER_TABLE)
				.append(Key.AFTER);
		boolean found = exchange.previous();
		long value;
		if (found) {
			value = exchange.getKey().indexTo(-1).decodeLong();
		} else {
			value = -1;
		}
		return value;
	}

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

	private PersistitStoreSession getSession() {
		PersistitStoreSession session = sessionLocal.get();
		if (session == null) {
			session = new PersistitStoreSession(db);
			sessionLocal.set(session);
		}
		return session;
	}

	private Exchange getExchange(final String treeName)
			throws PersistitException {
		return getSession().getExchange(VOLUME_NAME, treeName);
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
						.getParentJoinFields(), parentRowDef.getRowDefId());
			} else {
				//
				// Yes grandparent. Use pkIndex lookup.
				//
				final Exchange indexExchange = getExchange(parentRowDef
						.getPkTreeName());
				final Key indexKey = indexExchange.getKey();
				indexKey.clear();
				appendKeyFields(indexKey, rowDef, rowData, rowDef
						.getParentJoinFields(), parentRowDef.getRowDefId());
				if (!indexExchange.next(true)) {
					throw new StoreException(FOREIGN_KEY_MISSING, indexKey
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

	private void appendKeyFields(final Key key, final RowDef rowDef,
			final RowData rowData, final int[] fields, final int rowDefId)
			throws PersistitException {
		for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
			final FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
			final long location = rowDef.fieldLocation(rowData,
					fields[fieldIndex]);
			key.append(rowDefId % MAX_VERSIONS_PER_TABLE);
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
		case CHAR:
		case VARCHAR:
			final int start = (int) location;
			final int length = (int) (location >>> 32);
			key.append(new String(rowData.getBytes(), start, length));
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

	private class RowDataDisplayFilter implements DisplayFilter {
		private DisplayFilter defaultFilter;

		public RowDataDisplayFilter(final DisplayFilter filter) {
			this.defaultFilter = filter;
		}

		public String toKeyDisplayString(final Exchange exchange) {
			return defaultFilter.toKeyDisplayString(exchange);
		}

		public String toValueDisplayString(final Exchange exchange) {
			if (exchange.getTree().getVolume().getPathName().contains("aktest")
					&& !exchange.getTree().getName().startsWith("_txn")
					&& !exchange.getTree().getName().endsWith("_pk")) {
				final Value value = exchange.getValue();

				final int size = value.getEncodedSize() + RowData.ENVELOPE_SIZE;
				final byte[] bytes = new byte[size];
				Util.putInt(bytes, RowData.O_LENGTH_A, size);
				Util.putChar(bytes, RowData.O_SIGNATURE_A, RowData.SIGNATURE_A);
				System.arraycopy(value.getEncodedBytes(), 0, bytes,
						RowData.O_FIELD_COUNT, value.getEncodedSize());
				Util.putChar(bytes, size + RowData.O_SIGNATURE_B,
						RowData.SIGNATURE_B);
				Util.putInt(bytes, size + RowData.O_LENGTH_B, size);

				final RowData rowData = new RowData(bytes);
				rowData.prepareRow(0);
				return rowData.toString(rowDefCache);
			} else {
				return defaultFilter.toValueDisplayString(exchange);
			}
		}
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
			throw new IllegalArgumentException("Start and end RowData must specify the same rowDefId");
		}
		final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
		final Exchange exchange = getExchange(rowDef.getTreeName());
		final KeyFilter keyFilter = new KeyFilter(); // TODO - compute KeyFilter given start/end RowData values
		final RowCollector rc = new PersistitRowCollector(exchange, keyFilter, columnBitMap);
		getSession().setCurrentRowCollector(rc);
		return rc;
	}

	@Override
	public long getRowCount(final boolean exact, final RowData start,
			final RowData end, final byte[] columnBitMap) {
		// TODO Auto-generated method stub
		return 0;
	}

	private static class PersistitRowCollector implements RowCollector {

		private final static int INITIAL_BUFFER_SIZE = 1024;
		
		private final Exchange exchange;

		private final KeyFilter keyFilter;

		private final byte[] columnBitMap;

		private boolean putBack;
		
		private byte[] buffer = new byte[INITIAL_BUFFER_SIZE];

		private Key.Direction direction = Key.GTEQ;

		private PersistitRowCollector(final Exchange exchange,
				final KeyFilter keyFilter, final byte[] columnBitMap) {
			this.exchange = exchange;
			this.keyFilter = keyFilter;
			this.columnBitMap = columnBitMap;
		}

		@Override
		public boolean collectNextRow(ByteBuffer payload, byte[] columnBitMap)
				throws Exception {
			boolean more = false;
			int available = payload.limit() - payload.position();
			if (available < 4) {
				throw new IllegalStateException(
						"Payload byte buffer must have at least 4 "
								+ "bytes available, but actually has only "
								+ available);
			}
			if (putBack) {
				putBack = false;
				more = true;
			} else {
				more = exchange.traverse(direction, keyFilter,
						Integer.MAX_VALUE);
			}
			if (!more) {
				payload.putInt(0);
				return false;
			} else {
				int rowDataSize = exchange.getValue().getEncodedSize()
						+ RowData.ENVELOPE_SIZE;
				if (rowDataSize + 4 <= available) {
					if (rowDataSize > buffer.length){
						buffer = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
					}
					Util.putInt(buffer, RowData.O_LENGTH_A, rowDataSize);
					Util.putChar(buffer, RowData.O_SIGNATURE_A, RowData.SIGNATURE_A);
					System.arraycopy(exchange.getValue().getEncodedBytes(), 0, buffer, RowData.O_FIELD_COUNT, exchange.getValue().getEncodedSize());
					Util.putInt(buffer, RowData.O_SIGNATURE_B + rowDataSize, RowData.SIGNATURE_B);
					Util.putInt(buffer, RowData.O_LENGTH_B + rowDataSize, rowDataSize);
					payload.put(buffer, 0, rowDataSize);
					return true;
				} else {
					putBack = true;
					payload.putInt(-1);
					return false;
				}
			}
		}

		@Override
		public boolean hasMore() throws Exception {
			if (putBack) {
				putBack = false;
				return true;
			} else {
				boolean more = exchange.traverse(direction, keyFilter,
						Integer.MAX_VALUE);
				if (more) {
					putBack = true;
				}
				return more;
			}
		}
	}

}
