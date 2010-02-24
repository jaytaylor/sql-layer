package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.CorruptRowDataException;
import com.akiban.message.Request;

public class CServerShutdownRequest extends Request {

	public static short TYPE;

	public CServerShutdownRequest() {
		super(TYPE);
	}

	@Override
	public void read(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.read(payload);
	}

	@Override
	public void write(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.write(payload);
	}
}
