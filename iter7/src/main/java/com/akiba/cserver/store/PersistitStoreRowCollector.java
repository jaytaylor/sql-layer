/**
 * 
 */
package com.akiba.cserver.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.MySQLErrorConstants;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;

public class PersistitStoreRowCollector implements RowCollector,
		MySQLErrorConstants {

	static final Log LOG = LogFactory.getLog(PersistitStoreRowCollector.class
			.getName());

	private final static int INITIAL_BUFFER_SIZE = 1024;

	private final PersistitStore store;

	private final byte[] columnBitMap;

	private final RowDef rowDef;

	private final int leafRowDefId;

	private final KeyFilter keyFilter;

	private final KeyFilter indexKeyFilter;

	final int[] userRowDefIds;

	final int[] keySegments;

	final int[] keyDepth;

	final int[] columnOffset;

	int indexLevel = 0;

	private Exchange indexEx;

	private Exchange deliveryEx;

	private Exchange leafEx;

	private boolean more = true;

	private byte[] buffer = new byte[INITIAL_BUFFER_SIZE];

	private final RowData rowData = new RowData(buffer);

	private Key.Direction direction = Key.GTEQ;

	PersistitStoreRowCollector(PersistitStore store, final RowData start,
			final RowData end, final byte[] columnBitMap, RowDef rowDef)
			throws Exception {
		final String volumeName = PersistitStore.VOLUME_NAME; // TODO -
		this.store = store;
		this.columnBitMap = columnBitMap;
		this.rowDef = rowDef;
		this.leafRowDefId = computeLeafRowDefId(rowDef, columnBitMap);
		final int depth = depth(leafRowDefId);
		this.userRowDefIds = new int[depth];
		this.keySegments = new int[depth];
		this.keyDepth = new int[depth];
		this.columnOffset = new int[depth];
		this.analyzeLevels(leafRowDefId, rowDef, columnBitMap);
		this.keyFilter = computeKeyFilter(start, end);
		this.indexKeyFilter = computeIndexKeyFilter();
		this.deliveryEx = store.getSession().getExchange1(volumeName,
				rowDef.getTreeName());
		this.leafEx = store.getSession().getExchange2(volumeName,
				rowDef.getTreeName());
		this.indexEx = indexLevel == 0 ? null : store.getSession()
				.getExchange3(volumeName, indexTreeName());
		if (LOG.isInfoEnabled()) {
			LOG.info("Starting Scan on rowDef=" + rowDef.toString()
					+ ": leafRowDefId=" + leafRowDefId);
		}
		next = Next.NEXT_INDEXED_KEY;
	}

	/**
	 * Computes the rowDefId of the rightmost table whose columns are referenced
	 * in the columnBitMap. For example in the COI example, if the columnBitMap
	 * specifies only C and O columns, then this method returns the rowDefId of
	 * the O table. A similar computation in the MySQL head lets the CSClient
	 * know that upon receiving an O row, a completed result COI row can be
	 * generated.
	 * 
	 * @param rowDef
	 * @param columnBitMap
	 * @return rowDefId selected as described above
	 */
	int computeLeafRowDefId(final RowDef rowDef, final byte[] columnBitMap) {
		if (!rowDef.isGroupTable()) {
			return rowDef.getRowDefId();
		} else {
			int deepestRowDefId = -1;
			int rightmostColumn = -1;
			for (int column = rowDef.getFieldCount(); --column >= 0;) {
				if (isBit(columnBitMap, column)) {
					rightmostColumn = column;
					break;
				}
			}
			for (int index = 0; index < rowDef.getUserRowColumnOffsets().length; index++) {
				if (rowDef.getUserRowColumnOffsets()[index] > rightmostColumn) {
					break;
				}
				deepestRowDefId = rowDef.getUserRowDefIds()[index];
			}
			return deepestRowDefId;
		}
	}

	/**
	 * Compute and return the depth of a table in the group table tree. For
	 * example, in the C, A, O, I example, C has depth 1, A and O have depth 2
	 * and I has depth 2.
	 * 
	 * @param leafRowDefId
	 * @return depth
	 */
	int depth(final int leafRowDefId) {
		int depth = 0;
		int rowDefId = leafRowDefId;
		for (; rowDefId != 0;) {
			final RowDef rowDef = store.getRowDefCache().getRowDef(rowDefId);
			assert !rowDef.isGroupTable();
			rowDefId = rowDef.getParentRowDefId();
			depth++;
		}
		return depth;
	}

	/**
	 * Fill in three arrays containing information about each of the levels.
	 * <dl>
	 * <dt>userRowDefIds</dt>
	 * <dd>RowDefId values for the tree path, e.g., RowDefIds for C, O and I.</dd>
	 * <dt>keyDepth</dt>
	 * <dd>Number of h-key segments needed to express the h-key for rows at each
	 * level. For C, O, I, these values would be {2, 4, 6}. Each table
	 * contributes one plus the number of elements in its pkFields array. The
	 * values in the array represent the cumulative key depth.</dd>
	 * <dt>columnIndex</dt>
	 * <dd>the column number in the group table of the first column of the
	 * constituent group</dd>.
	 * </dl>
	 * 
	 * @param leafRowDefId
	 * @param rowDef
	 *            The RowDef of the start/end RowData instances
	 */
	void analyzeLevels(final int leafRowDefId, final RowDef rowDef,
			final byte[] columnBitMap) {
		int rowDefId = leafRowDefId;
		rowDefId = leafRowDefId;
		for (int index = userRowDefIds.length; --index >= 0;) {
			final RowDef userRowDef = store.getRowDefCache()
					.getRowDef(rowDefId);
			assert !userRowDef.isGroupTable();
			userRowDefIds[index] = rowDefId;
			keySegments[index] = userRowDef.getPkFields().length + 1;
			columnOffset[index] = computeColumnOffset(userRowDef, rowDef,
					columnBitMap);

			rowDefId = userRowDef.getParentRowDefId();
		}
		int depth = 0;
		for (int index = 0; index < keyDepth.length; index++) {
			depth = depth + keySegments[index];
			keyDepth[index] = depth;
		}
	}

	int computeColumnOffset(final RowDef userRowDef, final RowDef rowDef,
			final byte[] columnBitMap) {
		int columnOffset = -1;
		if (rowDef.isGroupTable()) {
			for (int index = 0; index < rowDef.getUserRowDefIds().length; index++) {
				if (rowDef.getUserRowDefIds()[index] == userRowDef
						.getRowDefId()) {
					columnOffset = rowDef.getUserRowColumnOffsets()[index];
					break;
				}
			}
			if (columnOffset == -1) {
				throw new IllegalStateException(
						"Broken AIS: No column offset for " + userRowDef
								+ " in group " + rowDef);
			}
		} else {
			if (userRowDef == rowDef) {
				columnOffset = 0;
			} else {
				return -1;
			}
		}
		boolean projected = false;
		for (int column = columnOffset; !projected
				&& column < columnOffset + userRowDef.getFieldCount(); column++) {
			if (isBit(columnBitMap, column)) {
				projected = true;
			}
		}
		return projected ? columnOffset : -1;
	}

	KeyFilter computeKeyFilter(final RowData start, final RowData end)
			throws Exception {
		final KeyFilter.Term[] terms = new KeyFilter.Term[keyDepth[keyDepth.length - 1]];
		int index = 0;
		for (int level = 0; level < userRowDefIds.length; level++) {
			final RowDef userRowDef = store.getRowDefCache().getRowDef(
					userRowDefIds[level]);
			terms[index++] = KeyFilter.simpleTerm(Integer
					.valueOf(userRowDefIds[level]));
			boolean all = false;
			for (int pkFieldIndex = 0; pkFieldIndex < userRowDef.getPkFields().length; pkFieldIndex++) {
				if (columnOffset[level] < 0) {
					all = true;
				}
				KeyFilter.Term term;
				if (!all) {
					term = computeKeyFilterTerm(rowDef, start, end, userRowDef
							.getPkFields()[pkFieldIndex]
							+ columnOffset[level]);
					if (term == KeyFilter.ALL) {
						all = true;
					} else {
						indexLevel = level;
					}
				} else {
					term = KeyFilter.ALL;
				}
				terms[index++] = term;
			}
		}
		return new KeyFilter(terms, 0, terms.length);
	}

	KeyFilter computeIndexKeyFilter() {
		if (indexLevel == 0) {
			return null;
		}
		final int at = keyDepth[indexLevel - 1];
		final int size = keyDepth[indexLevel];
		KeyFilter.Term[] terms = new KeyFilter.Term[size];
		for (int index = 0; index < at; index++) {
			terms[index + size - at] = keyFilter.getTerm(index);
		}
		for (int index = at; index < size; index++) {
			terms[index - at] = keyFilter.getTerm(index);
		}
		return new KeyFilter(terms, size, size);
	}

	String indexTreeName() {
		RowDef indexRowDef = store.getRowDefCache().getRowDef(
				userRowDefIds[indexLevel]);
		return indexRowDef.getPkTreeName();
	}

	/**
	 * Returns a KeyFilter term if the specified field of either the start or
	 * end RowData is non-null, else null.
	 * 
	 * @param rowDef
	 * @param start
	 * @param end
	 * @param fieldIndex
	 * @return
	 */
	KeyFilter.Term computeKeyFilterTerm(final RowDef rowDef,
			final RowData start, final RowData end, final int fieldIndex) {
		final long lowLoc = rowDef.fieldLocation(start, fieldIndex);
		final long highLoc = rowDef.fieldLocation(end, fieldIndex);
		if (lowLoc != 0 || highLoc != 0) {
			final Key key = new Key(store.getDb());
			key.clear();
			key.reset();
			if (lowLoc != 0) {
				store.appendKeyField(key, rowDef.getFieldDef(fieldIndex),
						start, lowLoc);
			} else {
				key.append(Key.BEFORE);
			}
			if (highLoc != 0) {
				store.appendKeyField(key, rowDef.getFieldDef(fieldIndex), end,
						highLoc);
			} else {
				key.append(Key.AFTER);
			}
			// Tricky: termFromKeySegments reads successive key segments when
			// called this way.
			return KeyFilter.termFromKeySegments(key, key, true, true);
		} else {
			return KeyFilter.ALL;
		}
	}

	private boolean isBit(final byte[] columnBitMap, final int column) {
		if (columnBitMap == null) {
			if (LOG.isErrorEnabled()) {
				LOG.error("ColumnBitMap is null in ScanRowsRequest on table "
						+ rowDef);
			}
			return false;
		}
		if ((column / 8) >= columnBitMap.length || column < 0) {
			if (LOG.isErrorEnabled()) {
				LOG.error("ColumnBitMap is too short in "
						+ "ScanRowsRequest on table " + rowDef
						+ " columnBitMap has " + columnBitMap.length
						+ " bytes, but isBit is " + "trying to test bit "
						+ column);
			}
			return false;
		}
		return (columnBitMap[column / 8] & (1 << (column % 8))) != 0;
	}

	private enum Next {
		NEXT_INDEXED_KEY, NEXT_ANCESTOR, NEXT_DECENDANT, PENDING_ROW,
	}

	Next next = Next.NEXT_INDEXED_KEY;

	Next pending;

	/**
	 * Selects the next row in tree-traversal order and puts it into the payload
	 * ByteBuffer if there is room. Returns <tt>true</tt> if and only if a row
	 * was added to the payload ByteBuffer. As a side-effect, this method
	 * affects the value of more to indicate whether there are additional rows
	 * in the tree; {@link #hasMore()} returns this value.
	 * 
	 */
	@Override
	public boolean collectNextRow(ByteBuffer payload) throws Exception {
		int available = payload.limit() - payload.position();
		if (available < 4) {
			throw new IllegalStateException(
					"Payload byte buffer must have at least 4 "
							+ "available bytes: " + available);
		}

		while (true) {
			switch (next) {

			case NEXT_INDEXED_KEY:
				if (indexLevel == 0) {
					next = Next.NEXT_DECENDANT;
				} else {
					leafEx.getKey().copyTo(deliveryEx.getKey());
					more = indexEx.traverse(direction, indexKeyFilter, 0);
					direction = Key.GT;
					if (!more) {
						return false;
					}
					store.copyAndRotate(indexEx.getKey(), leafEx.getKey(),
							keySegments[indexLevel - 1]);
					next = Next.NEXT_ANCESTOR;
				}
				continue;

			case NEXT_ANCESTOR:
				final Key deliveryKey = deliveryEx.getKey();
				int unique = deliveryKey.firstUniqueByteIndex(leafEx.getKey());
				leafEx.getKey().copyTo(deliveryKey);
				int levelRowDefId = -1;
				for (int level = 0; level < columnOffset.length; level++) {
					deliveryKey.indexTo(keyDepth[level]);
					if (columnOffset[level] >= 0
							&& deliveryKey.getIndex() > unique) {
						deliveryKey.setEncodedSize(deliveryKey.getIndex());
						levelRowDefId = userRowDefIds[level];
						break;
					}
				}
				if (levelRowDefId == -1) {
					// direction = Key.GTEQ;
					next = Next.NEXT_DECENDANT;
				} else {
					deliveryEx.fetch();
					prepareRow(deliveryEx, payload, levelRowDefId);
					arm();
				}
				continue;

			case NEXT_DECENDANT:
				boolean found = deliveryEx.traverse(direction, keyFilter,
						Integer.MAX_VALUE);
				direction = Key.GT;

				if (indexLevel > 0
						&& (!found || deliveryEx.getKey().firstUniqueByteIndex(
								leafEx.getKey()) < leafEx.getKey()
								.getEncodedSize())) {
					next = Next.NEXT_INDEXED_KEY;
					continue;
				}

				if (!found) {
					more = false;
					return false;
				}
				int depth = deliveryEx.getKey().getDepth();
				int expectedRowDefId = leafRowDefId;
				if (depth < keyFilter.getMaximumDepth()) {
					expectedRowDefId = -1;
					for (int index = 0; index < keyDepth[index]; index++) {
						if (depth == keyDepth[index]) {
							if (columnOffset[index] >= 0) {
								expectedRowDefId = userRowDefIds[index];
							}
							break;
						}
					}
				}
				if (expectedRowDefId != -1) {
					prepareRow(deliveryEx, payload, expectedRowDefId);
					arm();
				}
				continue;

			case PENDING_ROW:
				if (deliverRow(payload)) {
					next = pending;
					return true;
				} else {
					return false;
				}

			default:
				assert false : "Missing case";
			}
		}
	}

	private void arm() {
		pending = next;
		next = Next.PENDING_ROW;
	}

	void prepareRow(final Exchange exchange, final ByteBuffer payload,
			final int expectedRowDefId) throws Exception {
		final byte[] bytes = exchange.getValue().getEncodedBytes();
		final int size = exchange.getValue().getEncodedSize();
		int rowDataSize = size + RowData.ENVELOPE_SIZE;

		if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH) {
			if (PersistitStore.LOG.isErrorEnabled()) {
				PersistitStore.LOG.error("Value at " + exchange.getKey()
						+ " is not a valid row - skipping");
			}
			throw new StoreException(HA_ERR_INTERNAL_ERROR,
					"Corrupt RowData at " + exchange.getKey());
		} else {
			final int rowDefId = CServerUtil.getInt(bytes, RowData.O_ROW_DEF_ID
					- RowData.LEFT_ENVELOPE_SIZE);
			if (rowDefId != expectedRowDefId) {
				//
				// Add code to here to evolve data to required expectedRowDefId
				//
				throw new StoreException(HA_ERR_INTERNAL_ERROR,
						"Unable to convert rowDefId " + rowDefId
								+ " to expected rowDefId " + expectedRowDefId);
			}
			if (rowDataSize > buffer.length) {
				buffer = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
				rowData.reset(buffer);
			}
			//
			// Assemble the Row in a byte array to allow column
			// elision
			//
			CServerUtil.putInt(buffer, RowData.O_LENGTH_A, rowDataSize);
			CServerUtil.putChar(buffer, RowData.O_SIGNATURE_A,
					RowData.SIGNATURE_A);
			System
					.arraycopy(exchange.getValue().getEncodedBytes(), 0,
							buffer, RowData.O_FIELD_COUNT, exchange.getValue()
									.getEncodedSize());
			CServerUtil.putChar(buffer, RowData.O_SIGNATURE_B + rowDataSize,
					RowData.SIGNATURE_B);
			CServerUtil.putInt(buffer, RowData.O_LENGTH_B + rowDataSize,
					rowDataSize);
			rowData.prepareRow(0);
		}
	}

	boolean deliverRow(final ByteBuffer payload) throws IOException {
		if (rowData.getRowSize() + 4 < payload.limit() - payload.position()) {
			payload.put(rowData.getBytes(), rowData.getRowStart(), rowData
					.getRowSize());
			if (LOG.isDebugEnabled()) {
				LOG.debug("collectNextRow returned: "
						+ rowData.toString(store.getRowDefCache()));
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean hasMore() {
		return more;
	}
}