package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.message.Message;
import com.akiban.message.Response;

public class DropTableResponse extends Response
{
	
	public static short TYPE;
	
	private int resultCode;

	public int getResultCode() {
		return resultCode;
	}

	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}

	public DropTableResponse() {
		super(TYPE);
	}
	
	public DropTableResponse(final int resultCode) {
		super(TYPE);
		this.resultCode = resultCode;
	}

	@Override
	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		resultCode = payload.getInt();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception
    {
		super.write(payload);
		payload.putInt(resultCode);
	}

}
