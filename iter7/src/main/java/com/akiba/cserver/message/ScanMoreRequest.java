package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.cserver.CorruptRowDataException;
import com.akiba.cserver.CServer.CServerContext;
import com.akiba.cserver.store.RowCollector;
import com.akiba.cserver.store.Store;
import com.akiba.message.AkibaConnection;
import com.akiba.message.ExecutionContext;
import com.akiba.message.Message;

public class ScanMoreRequest extends Message {

	public static short TYPE;

	private int sessionId;

	public ScanMoreRequest() {
		super(TYPE);
	}
	
	public int getSessionId() {
		return sessionId;
	}

	public void setSessionId(int sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public void execute(final AkibaConnection connection,
			ExecutionContext context) throws Exception {
		final Store store = ((CServerContext) context).getStore();
		final RowCollector collector = store.getRowCollector(sessionId);
		final ScanResponse response = new ScanResponse(sessionId, collector);
		//
		// Note: the act of serializing the response message invokes
		// the RowCollector to actually scan the rows. This lets
		// the RowCollector copy bytes directly into the response ByteBuffer.
		//
		connection.send(response);
	}

	@Override
	public void read(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.read(payload);
		sessionId = payload.getInt();
	}

	@Override
	public void write(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.write(payload);
		payload.putInt(sessionId);
	}



}
