package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.cserver.store.RowCollector;
import com.akiba.cserver.store.RowDistributor;
import com.akiba.message.Message;

public class ScanRowsResponse extends Message {

	public static short TYPE;
	
	private short resultCode;

	private byte[] columnBitMap;
	
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
		final int size = payload.getChar();
		columnBitMap = new byte[size];
		payload.get(columnBitMap);
		while(distributor.distributeNextRow(payload, columnBitMap));
	}

	@Override
	public void write(ByteBuffer payload) throws Exception {
		super.write(payload);
		payload.putShort(resultCode);
		payload.putChar((char)columnBitMap.length);
		payload.put(columnBitMap);
		while (collector.collectNextRow(payload));
		payload.putInt(collector.hasMore() ? -1 : 0);
	}
}
