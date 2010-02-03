/**
 * 
 */
package com.akiba.cserver.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.MySQLErrorConstants;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;

public class PersistitStoreRowCollector implements RowCollector, MySQLErrorConstants {

	static final Log LOG = LogFactory.getLog(PersistitStoreRowCollector.class
			.getName());

	private final static int INITIAL_BUFFER_SIZE = 1024;

	private final PersistitStore store;

	private final KeyFilter keyFilter;

	private final byte[] columnBitMap;
	
	private final RowDef rowDef;

	private final int leafRowDefId;

	private Exchange pkExchange;

	private final Exchange exchange;

	private boolean more = true;

	private byte[] buffer = new byte[INITIAL_BUFFER_SIZE];

	private final RowData rowData = new RowData(buffer);
	
	private Key.Direction direction = Key.GTEQ;

	PersistitStoreRowCollector(PersistitStore store, final RowData start,
			final RowData end, final byte[] columnBitMap, RowDef rowDef)
			throws Exception {
		this.exchange = store.getExchange(rowDef.getTreeName());
		this.pkExchange = null;
		this.store = store;
		this.columnBitMap = columnBitMap;
		this.rowDef = rowDef;
		this.leafRowDefId = projectionLeafRowDefId(rowDef, columnBitMap);
		this.keyFilter = computeKeyFilter(rowDef, start, end);
		if (LOG.isInfoEnabled()) {
			LOG.info("Starting Scan on rowDef=" + rowDef.toString()
					+ ": leafRowDefId=" + leafRowDefId);
		}
		traverseToNextRow(true);
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
	int projectionLeafRowDefId(final RowDef rowDef, final byte[] columnBitMap) {
		if (!rowDef.isGroupTable()) {
			return rowDef.getRowDefId();
		} else {
			int deepestRowDefId = -1;
			int rightmostColumn = -1;
			for (int index = 0; index < columnBitMap.length; index++) {
				for (int bit = 0; bit < 8; bit++) {
					if ((columnBitMap[index] & (1 << bit)) != 0) {
						rightmostColumn = index * 8 + bit;
					}
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

	KeyFilter computeKeyFilter(final RowDef rowDef, final RowData start,
			final RowData end) throws Exception {

		final List<KeyFilter.Term> terms = new ArrayList<KeyFilter.Term>();
		if (rowDef.isGroupTable()) {
			for (int tableIndex = rowDef.getUserRowDefIds().length; --tableIndex >= 0; ) {
				final RowDef userRowDef = store.getRowDefCache().getRowDef(rowDef.getUserRowDefIds()[tableIndex]);
				for (int index = 0; index < userRowDef.getPkFields().length; index++) {
					final int userRowColumnOffset = rowDef.getUserRowColumnOffsets()[tableIndex];
					final int pkFieldIndex = userRowDef.getPkFields()[index];
					final int fieldIndex = pkFieldIndex + userRowColumnOffset;
					KeyFilter.Term term = computeKeyFilterTerm(rowDef, start, end, fieldIndex);
					if (term == null) {
						break;
					}
					if (pkExchange == null) {
						pkExchange = store.getExchange(userRowDef.getPkTreeName());
					}
					if (index == 0) {
						terms.add(KeyFilter.simpleTerm(Integer.valueOf(userRowDef.getRowDefId())));
					}
					terms.add(term);
					tableIndex = -1; // all done
				}
			}
		} else {
			for (int index = 0; index < rowDef.getPkFields().length; index++) {
				int pkFieldIndex = rowDef.getPkFields()[index];
				KeyFilter.Term term = computeKeyFilterTerm(rowDef, start, end, pkFieldIndex);
				if (term == null) {
					break;
				}
				if (pkExchange == null) {
					pkExchange = store.getExchange(rowDef.getPkTreeName());
				}
				if (index == 0) {
					terms.add(KeyFilter.simpleTerm(Integer.valueOf(rowDef.getRowDefId())));
				}
				terms.add(term);
			}
		}
		final KeyFilter.Term[] termArray = new KeyFilter.Term[terms.size()];
		return new KeyFilter(terms.toArray(termArray),terms.size(), Integer.MAX_VALUE);
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
			// called
			// this way.
			return KeyFilter.termFromKeySegments(key, key, true, true);
		} else {
			return null;
		}
	}

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
							+ "available bytes, but has only " + available);
		}
		while (more) {
			if (pkExchange != null) {
				// fetch the row
				store.copyAndRotate(pkExchange.getKey(), exchange.getKey(), keyFilter.getMinimumDepth());
				exchange.fetch();
				if (!exchange.getValue().isDefined()) {
					if (LOG.isErrorEnabled()) {
						LOG.error("Index key " + pkExchange + " has no h-value in " + exchange);
					}
					throw new StoreException(HA_ERR_KEY_NOT_FOUND, "Index key " + pkExchange + " has no h-value in " + exchange);
				}
			}
			final byte[] bytes = exchange.getValue().getEncodedBytes();
			final int size = exchange.getValue().getEncodedSize();
			int rowDataSize = size + RowData.ENVELOPE_SIZE;
			if (rowDataSize + 4 <= available) {
				if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH) {
					if (PersistitStore.LOG.isErrorEnabled()) {
						PersistitStore.LOG.error("Value at "
								+ exchange.getKey()
								+ " is not a valid row - skipping");
					}
					traverseToNextRow(false);
					continue;
				}
				final int rowDefId = CServerUtil.getInt(bytes,
						RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE);

				//
				// Handle SELECT on a user table
				//
				if (!rowDef.isGroupTable()) {
					if (rowDefId == leafRowDefId) {
						prepareNextRowBuffer(payload, rowDataSize);
						payload.put(buffer, 0, rowDataSize);
						store.logRowData("ScanRowsResponse adding ", rowData);
						traverseToNextRow(false);
						break;
					} else {
						traverseToNextRow(true);
						continue;
					}
				}

				// Handle SELECT on a group table
				//
				// Copy the row into the buffer in case we
				// decide to return it.
				//
				prepareNextRowBuffer(payload, rowDataSize);
				//
				if (rowDefId == leafRowDefId) {
					payload.put(buffer, 0, rowDataSize);
					exchange.getKey().append(Key.AFTER);
					traverseToNextRow(false);
					break;
				} else {
					//
					// Find out of there's a child row. If so
					// then we can return this row.
					//
					final int myDepth = exchange.getKey().getDepth();
					traverseToNextRow(true);
					if (more && exchange.getKey().getDepth() > myDepth) {
						payload.put(buffer, 0, rowDataSize);
						break;
					}
					continue;
				}
			} else {
				return false;
			}
		}
		return more;
	}

	@Override
	public boolean hasMore() {
		return more;
	}

	private void prepareNextRowBuffer(ByteBuffer payload, int rowDataSize) {
		if (rowDataSize > buffer.length) {
			buffer = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
			rowData.reset(buffer);
		}
		//
		// Assemble the Row in a byte array to allow column
		// elision
		//
		CServerUtil.putInt(buffer, RowData.O_LENGTH_A, rowDataSize);
		CServerUtil.putChar(buffer, RowData.O_SIGNATURE_A, RowData.SIGNATURE_A);
		System.arraycopy(exchange.getValue().getEncodedBytes(), 0, buffer,
				RowData.O_FIELD_COUNT, exchange.getValue().getEncodedSize());
		CServerUtil.putChar(buffer, RowData.O_SIGNATURE_B + rowDataSize,
				RowData.SIGNATURE_B);
		CServerUtil.putInt(buffer, RowData.O_LENGTH_B + rowDataSize,
				rowDataSize);
	}

	private void traverseToNextRow(final boolean deeper) throws Exception {
		if (pkExchange != null) {
			more = pkExchange.traverse(direction, keyFilter, Integer.MAX_VALUE);
		} else {
			if (!deeper) {
				// optimization to skip any child rows
				exchange.append(Key.AFTER);
			}
			more = exchange.traverse(direction, keyFilter, Integer.MAX_VALUE);
		}
		direction = Key.GT;
	}
}