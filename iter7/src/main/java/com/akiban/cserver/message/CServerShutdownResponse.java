package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.message.Response;

public class CServerShutdownResponse extends Response
{

    public static short TYPE;

	private int resultCode;
	
	public int getResultCode() {
		return resultCode;
	}

	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}

	public CServerShutdownResponse() {
		super(TYPE);
	}
	
	public CServerShutdownResponse(final int resultCode) {
		super(TYPE);
		setResultCode(resultCode);
	}

	@Override
	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		resultCode = payload.getShort();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception
    {
		super.write(payload);
		payload.putShort((short)resultCode);
	}
}
