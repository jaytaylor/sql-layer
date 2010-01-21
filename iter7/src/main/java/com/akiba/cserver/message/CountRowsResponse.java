package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.message.Message;

public class CountRowsResponse extends Message {

	public static short TYPE;
	
	private int sessionId;
	
	private long count;
	
	public int getSessionId() {
		return sessionId;
	}

	public void setSessionId(int sessionId) {
		this.sessionId = sessionId;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public CountRowsResponse() {
		super(TYPE);
	}
	
	public CountRowsResponse(final int sessionId, final long count) {
		super(TYPE);
		this.sessionId = sessionId;
		this.count = count;
	}
	
	@Override
	public void read(ByteBuffer payload) throws Exception {
		super.read(payload);
		sessionId = payload.getInt();
		count = payload.getLong();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception {
		super.write(payload);
		payload.putInt(sessionId);
		payload.putLong(count);
	}
}
