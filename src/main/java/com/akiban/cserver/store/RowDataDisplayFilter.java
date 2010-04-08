/**
 * 
 */
package com.akiban.cserver.store;

import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.RowData;
import com.persistit.Exchange;
import com.persistit.Value;
import com.persistit.Management.DisplayFilter;

class RowDataDisplayFilter implements DisplayFilter {

	private final Store store;

	private DisplayFilter defaultFilter;

	public RowDataDisplayFilter(Store store, final DisplayFilter filter) {
		this.store = store;
		this.defaultFilter = filter;
	}

	public String toKeyDisplayString(final Exchange exchange) {
		return defaultFilter.toKeyDisplayString(exchange);
	}

	public String toValueDisplayString(final Exchange exchange) {
		if (exchange.getTree().getVolume().getPathName().contains("_data")
				&& !exchange.getTree().getName().contains("_status_") 
				&& !exchange.getTree().getName().contains("$$")) {
			final Value value = exchange.getValue();
			final int size = value.getEncodedSize() + RowData.ENVELOPE_SIZE;
			final byte[] bytes = new byte[size];
			CServerUtil.putInt(bytes, RowData.O_LENGTH_A, size);
			CServerUtil.putChar(bytes, RowData.O_SIGNATURE_A,
					RowData.SIGNATURE_A);
			System.arraycopy(value.getEncodedBytes(), 0, bytes,
					RowData.O_FIELD_COUNT, value.getEncodedSize());
			CServerUtil.putChar(bytes, size + RowData.O_SIGNATURE_B,
					RowData.SIGNATURE_B);
			CServerUtil.putInt(bytes, size + RowData.O_LENGTH_B, size);

			final RowData rowData = new RowData(bytes);
			rowData.prepareRow(0);
			return rowData.toString(store.getRowDefCache());

		} else {
			return defaultFilter.toValueDisplayString(exchange);
		}
	}
}