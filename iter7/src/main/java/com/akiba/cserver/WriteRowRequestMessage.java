package com.akiba.cserver;

import java.nio.ByteBuffer;

import com.akiba.message.Message;

public class WriteRowRequestMessage extends Message {

    public static short TYPE;

	private RowData rowData;

	public WriteRowRequestMessage() {
		super(TYPE);
	}

	public RowData getRowData() {
		return rowData;
	}

	public void setRowData(RowData rowData) {
		this.rowData = rowData;
	}

	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		if (!payload.hasArray()) {
			throw new UnsupportedOperationException(
					"This version requires a heap-based ByteBuffer");
		}
		byte[] bytes = payload.array();
		rowData = new RowData(bytes, payload.position(), payload.limit()
				- payload.position());
	}

	public void write(ByteBuffer payload) throws Exception
    {
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
			payload.put(rowData.getBytes(), rowData.getBufferLength(), rowData
					.getBufferLength());
		}
	}
}
