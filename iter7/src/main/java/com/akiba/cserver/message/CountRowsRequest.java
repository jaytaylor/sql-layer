package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.cserver.CorruptRowDataException;
import com.akiba.cserver.RowData;
import com.akiba.cserver.CServer.CServerContext;
import com.akiba.cserver.store.RowCollector;
import com.akiba.cserver.store.Store;
import com.akiba.message.AkibaConnection;
import com.akiba.message.ExecutionContext;
import com.akiba.message.Message;
import com.persistit.Util;

public class CountRowsRequest extends Message {

	public static short TYPE;

	private int sessionId;
	
	private int indexId;
	
	private int accuracy;
	
	private byte[] columnBitMap;

	private RowData start;

	private RowData end;

	public CountRowsRequest() {
		super(TYPE);
	}
	
	public int getIndexId() {
		return indexId;
	}

	public void setIndexId(int indexId) {
		this.indexId = indexId;
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

	public int getAccuracy() {
		return accuracy;
	}

	public void setAccuracy(int accuracy) {
		this.accuracy = accuracy;
	}

	@Override
	public void execute(final AkibaConnection connection,
			ExecutionContext context) throws Exception {
		final Store store = ((CServerContext) context).getStore();
		final long count = store.getRowCount(accuracy, start, end, columnBitMap);
		final CountRowsResponse response = new CountRowsResponse(sessionId, count);
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
		indexId = payload.getInt();
		int columnBitMapLength = payload.getChar();
		columnBitMap = new byte[columnBitMapLength];
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
		//
		// Note - prepareRow can throw CorruptRowDataException
		//
		start.prepareRow(0);
		end.prepareRow(0);
		accuracy = payload.getInt();
	}

	@Override
	public void write(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.write(payload);
		start.validateRow(start.getRowStart());
		end.validateRow(end.getRowStart());
		payload.putInt(sessionId);
		payload.putInt(indexId);
		payload.putChar((char) columnBitMap.length);
		payload.put(columnBitMap);
		payload.put(start.getBytes(), start.getRowStart(), start.getRowSize());
		payload.put(end.getBytes(), end.getRowStart(), end.getRowSize());
		payload.putInt(accuracy);
	}

}
