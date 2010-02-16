package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.store.RowCollector;
import com.akiban.cserver.store.RowDistributor;
import com.akiban.message.Message;
import com.akiban.message.Response;

public class ScanRowsResponse extends Response
{

	public static short TYPE;
	
	private short resultCode;

	private RowCollector collector;
	
	private RowDistributor distributor;

	public ScanRowsResponse() {
		super(TYPE);
	}
	
	public ScanRowsResponse(final short resultCode, final RowCollector collector) {
		super(TYPE);
		this.resultCode = resultCode;
		this.collector = collector;
	}
	
	@Override
	public void read(ByteBuffer payload) throws Exception {
		super.read(payload);
		resultCode = payload.getShort();
		while(distributor.distributeNextRow(payload));
	}

	@Override
	public void write(ByteBuffer payload) throws Exception {
		super.write(payload);
		payload.putShort(resultCode);
		while (collector.collectNextRow(payload));
		payload.putInt(collector.hasMore() ? -1 : 0);
	}
}
