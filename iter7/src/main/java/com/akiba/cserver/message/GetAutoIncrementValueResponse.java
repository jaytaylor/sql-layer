package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.message.Message;
import com.akiba.message.Response;

public class GetAutoIncrementValueResponse extends Response
{
	
	public static short TYPE;
	
	private int rowDefId;
	
	private int resultCode;

	private long value;

	public int getRowDefId() {
		return rowDefId;
	}

	public void setRowDefId(final int rowDefId) {
		this.rowDefId = rowDefId;

	}
	
	public int getResultCode() {
		return resultCode;
	}

	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public GetAutoIncrementValueResponse() {
		super(TYPE);
	}
	
	public GetAutoIncrementValueResponse(final int rowDefId, final int resultCode, final long value) {
		super(TYPE);
		this.rowDefId = rowDefId;
		this.resultCode = resultCode;
		this.value = value;
	}

	@Override
	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		rowDefId = payload.getInt();
		resultCode = payload.getInt();
		value = payload.getLong();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception
    {
		super.write(payload);
		payload.putInt(rowDefId);
		payload.putInt(resultCode);
		payload.putLong(value);
	}

}
