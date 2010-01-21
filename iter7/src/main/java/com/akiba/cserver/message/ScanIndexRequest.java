package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.cserver.RowData;
import com.akiba.message.Message;
import com.persistit.Util;

public class ScanIndexRequest extends Message {

	public static short TYPE;
	
	private long sessionId;
	
	private byte[] columnBitMap;
	
	public long getSessionId() {
		return sessionId;
	}

	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}

	public byte[] getColumnBitMap() {
		return columnBitMap;
	}

	public void setColumnBitMap(byte[] columnBitMap) {
		this.columnBitMap = columnBitMap;
	}

	public RowData getStart() {
		return start;
	}

	public void setStart(RowData start) {
		this.start = start;
	}

	public RowData getEnd() {
		return end;
	}

	public void setEnd(RowData end) {
		this.end = end;
	}

	private RowData start;
	
	private RowData end;

	public ScanIndexRequest() {
		super(TYPE);
	}
	
	@Override
	public void read(final ByteBuffer payload) throws Exception {
		super.read(payload);
		sessionId = payload.getLong();
		int size = payload.getChar();
		columnBitMap = new byte[size];
		payload.get(columnBitMap);
		
		int startSize = payload.getInt();
		byte[] startBytes = new byte[startSize];
		Util.putInt(startBytes, 0, startSize);
		payload.get(startBytes, 4, startSize - 4);
		start = new RowData(startBytes);
		
		int endSize = payload.getInt();
		byte[] endBytes = new byte[endSize];
		Util.putInt(endBytes, 0, endSize);
		payload.get(endBytes, 4, endSize - 4);
		end = new RowData(endBytes);
		
		start.prepareRow(0);
		end.prepareRow(0);
	}
	
	@Override
	public void write(final ByteBuffer payload) throws Exception {
		super.write(payload);
		payload.putLong(sessionId);
		payload.putChar((char)columnBitMap.length);
	}

}
