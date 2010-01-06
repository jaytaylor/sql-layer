package com.akiba.cserver;

import java.nio.ByteBuffer;

import com.akiba.message.Message;

public class WriteRowResponse extends Message {

    public static short TYPE;

	private int resultCode;
	
	public int getResultCode() {
		return resultCode;
	}

	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}

	public WriteRowResponse() {
		super(TYPE);
	}

	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		resultCode = payload.getShort();
	}

	public void write(ByteBuffer payload) throws Exception
    {
		payload.putShort((short)resultCode);
	}
}
