package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CorruptRowDataException;
import com.akiban.cserver.CServer.CServerContext;
import com.akiban.cserver.store.RowCollector;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.Request;

public class ScanRowsMoreRequest extends Request implements CServerConstants {

	public static short TYPE;

	public ScanRowsMoreRequest() {
		super(TYPE);
	}
	
	@Override
	public void execute(final AkibaConnection connection,
			ExecutionContext context) throws Exception {
		final Store store = ((CServerContext) context).getStore();
		final RowCollector collector = store.getCurrentRowCollector();
		final ScanRowsResponse response = new ScanRowsResponse(OK, collector);
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
	}

	@Override
	public void write(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.write(payload);
	}



}
