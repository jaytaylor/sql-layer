package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.message.Message;
import com.akiba.message.Response;

public class CountRowsResponse extends Response
{

	public static short TYPE;
	
	private int resultCode;
	
	private long count;
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public int getResultCode() {
		return resultCode;
	}

	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}

	public CountRowsResponse() {
		super(TYPE);
	}
	
	public CountRowsResponse(final int resultCode, final long count) {
		super(TYPE);
		this.resultCode = resultCode;
		this.count = count;
	}
	
	@Override
	public void read(ByteBuffer payload) throws Exception {
		super.read(payload);
		resultCode = payload.getInt();
		count = payload.getLong();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception {
		super.write(payload);
		payload.putInt(resultCode);
		payload.putLong(count);
	}
}
