package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.cserver.store.RowCollector;
import com.akiba.cserver.store.RowDistributor;
import com.akiba.message.Message;

public class ScanResponse extends Message {

	public static short TYPE;
	
	private long sessionId;
	
	private byte[] columnBitMap;

	private RowCollector collector;
	
	private RowDistributor distributor;

	public ScanResponse() {
		super(TYPE);
	}
	
	public ScanResponse(final long sessionId, final RowCollector collector) {
		super(TYPE);
		this.sessionId = sessionId;
		this.collector = collector;
	}
	
	@Override
	public void read(ByteBuffer payload) throws Exception {
		super.read(payload);
		sessionId = payload.getLong();
		final int size = payload.getChar();
		columnBitMap = new byte[size];
		payload.get(columnBitMap);
		while(distributor.distributeNextRow(payload, columnBitMap));
		
	}

	@Override
	public void write(ByteBuffer payload) throws Exception {
		super.write(payload);
		payload.putLong(sessionId);
		payload.putChar((char)columnBitMap.length);
		payload.put(columnBitMap);
		while (collector.collectNextRow(payload, columnBitMap));
		payload.putInt(0);
	}
}
