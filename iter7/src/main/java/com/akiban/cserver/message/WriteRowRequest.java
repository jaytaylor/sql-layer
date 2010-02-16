package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.RowData;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.CServer.CServerContext;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.Request;

public class WriteRowRequest extends Request
{

	public static short TYPE;

	private RowData rowData;

	public WriteRowRequest() {
		super(TYPE);
	}

	public RowData getRowData() {
		return rowData;
	}

	public void setRowData(RowData rowData) {
		this.rowData = rowData;
	}

	@Override
	public void execute(final AkibaConnection connection,
			final ExecutionContext context) throws Exception {
		final Store store = ((CServerContext) context).getStore();
		final int result = store.writeRow(rowData);
		final Message message = new WriteRowResponse(result);
		connection.send(message);
	}

	@Override
	public void read(ByteBuffer payload) throws Exception {
		super.read(payload);
		if (!payload.hasArray()) {
			throw new UnsupportedOperationException(
					"This version requires a heap-based ByteBuffer");
		}
		byte[] bytes = payload.array();
		rowData = new RowData(bytes, payload.position(), payload.limit()
				- payload.position());
		rowData.prepareRow(rowData.getBufferStart());
	}

	@Override
	public void write(ByteBuffer payload) throws Exception {
		if (payload.hasArray() && payload.array() == rowData.getBytes()) {
			final byte[] bytes = rowData.getBytes();
			if (rowData.getBufferStart() != payload.position() + 2) {
				System.arraycopy(bytes, rowData.getBufferStart(), bytes,
						payload.position() + 2, rowData.getBufferLength());
				super.write(payload);
				payload
						.position(payload.position()
								+ rowData.getBufferLength());
			}
		} else {
			super.write(payload);
			payload.put(rowData.getBytes(), rowData.getBufferStart(), rowData
					.getBufferLength());
		}
	}

	@Override
	public String toString() {
		return String.format("WriteRowMessage bytes %d-%d: \n%s", rowData
				.getRowStart(), rowData.getRowEnd(), CServerUtil.dump(rowData
				.getBytes(), rowData.getRowStart(), rowData.getRowEnd()
				- rowData.getRowStart()));
	}
}
