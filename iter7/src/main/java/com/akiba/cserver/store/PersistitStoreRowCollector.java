/**
 * 
 */
package com.akiba.cserver.store;

import java.nio.ByteBuffer;

import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;

public class PersistitStoreRowCollector implements RowCollector {

	private final static int INITIAL_BUFFER_SIZE = 1024;

	private final PersistitStore store;

	private final KeyFilter keyFilter;

	private final byte[] columnBitMap;

	private final RowDef rowDef;

	private final int leafRowDefId;

	private final Exchange pkExchange;

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

		if (rowDef.getUserRowDefIds() == null) {
			leafRowDefId = rowDef.getRowDefId();
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
			leafRowDefId = deepestRowDefId;
		}
		exchange.clear();
		this.keyFilter = constructKeyFilter(start, end, rowDef);
		traverseToNextRow();
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
					traverseToNextRow();
					continue;
				}
				final int rowDefId = CServerUtil.getInt(bytes,
						RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE);

				//
				// Handle SELECT on a user table
				//
				if (rowDef.getUserRowDefIds() == null) {
					if (rowDef.getRowDefId() == rowDefId) {
						prepareNextRowBuffer(payload, rowDataSize);
						payload.put(buffer, 0, rowDataSize);
						traverseToNextRow();
						break;
					} else {
						traverseToNextRow();
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
					//
					// Optimization to traverse past unwanted child rows
					// 
					//bumpPastChildren();
					exchange.getKey().append(Key.AFTER);
					traverseToNextRow();
					break;
				} else {
					//
					// Find out of there's a child row. If so
					// then we can return this row.
					//
					final int myDepth = exchange.getKey().getDepth();
					traverseToNextRow();
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

	private void traverseToNextRow() throws Exception {
		if (pkExchange != null) {
			more = pkExchange.traverse(direction, keyFilter, Integer.MAX_VALUE);
		} else {
			more = exchange.traverse(direction, keyFilter, Integer.MAX_VALUE);
		}
		direction = Key.GT;
	}

	private KeyFilter constructKeyFilter(final RowData start,
			final RowData end, final RowDef rowDef) {
		final KeyFilter keyFilter = new KeyFilter();
		return keyFilter;
	}
}