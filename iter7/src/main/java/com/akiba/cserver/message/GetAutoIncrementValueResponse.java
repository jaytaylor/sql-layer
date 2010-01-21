package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.message.Message;

public class GetAutoIncrementValueResponse extends Message {
	
	public static short TYPE;
	
	private int rowDefId;
	
	public int getRowDefId() {
		return rowDefId;
	}

	public void setRowDefId(int rowDefId) {
		this.rowDefId = rowDefId;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	private long value;

	public GetAutoIncrementValueResponse() {
		super(TYPE);
	}
	
	public GetAutoIncrementValueResponse(final int rowDefId, final long value) {
		super(TYPE);
		this.rowDefId = rowDefId;
		this.value = value;
	}

	@Override
	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		rowDefId = payload.getInt();
		value = payload.getLong();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception
    {
		super.write(payload);
		payload.putInt(rowDefId);
		payload.putLong(value);
	}

}
