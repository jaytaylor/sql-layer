package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CorruptRowDataException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.CServer.CServerContext;
import com.akiban.cserver.store.RowCollector;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.cserver.CServerUtil;
import com.akiban.message.Request;

public class ScanRowsRequest extends Request implements CServerConstants {

	/*
	 * Sanity size for the RowData objects delimiting the start and end
	 * of the scan.
	 */
	private final static int MAX_LIMIT_ROWDATA_SIZE = 65536;

	public static short TYPE;
	
	private int indexId;
	
	private byte[] columnBitMap;

	private RowData start;

	private RowData end;

	public ScanRowsRequest() {
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

	@Override
	public void execute(final AkibaConnection connection,
			ExecutionContext context) throws Exception {
		final Store store = ((CServerContext) context).getStore();
		final RowCollector collector = store.newRowCollector(indexId, start, end,
				columnBitMap);
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
		indexId = payload.getInt();
		int columnBitMapLength = payload.getChar();
		columnBitMap = new byte[columnBitMapLength];
		payload.get(columnBitMap);

		start = decodeRowData(payload);
		end = decodeRowData(payload);
	}
	
	private RowData decodeRowData(final ByteBuffer payload) {
		byte[] sizeBuffer = new byte[4];
		payload.get(sizeBuffer);
		int rowDataSize = CServerUtil.getInt(sizeBuffer, 0);
		if (rowDataSize < RowData.ENVELOPE_SIZE || rowDataSize > MAX_LIMIT_ROWDATA_SIZE) {
			throw new IllegalStateException("Invalid RowData size: " + rowDataSize);
		}
		byte[] rowDataBytes = new byte[rowDataSize];
		System.arraycopy(sizeBuffer, 0, rowDataBytes, 0, sizeBuffer.length);
		
		CServerUtil.putInt(rowDataBytes, 0, rowDataSize);
		payload.get(rowDataBytes, 4, rowDataSize - 4);
		final RowData rowData = new RowData(rowDataBytes);
		rowData.prepareRow(0);
		return rowData;
	}

	@Override
	public void write(final ByteBuffer payload) throws Exception,
			CorruptRowDataException {
		super.write(payload);
		start.validateRow(start.getRowStart());
		end.validateRow(end.getRowStart());
		payload.putInt(indexId);
		payload.putChar((char) columnBitMap.length);
		payload.put(columnBitMap);
		payload.put(start.getBytes(), start.getRowStart(), start.getRowSize());
		payload.put(end.getBytes(), end.getRowStart(), end.getRowSize());
	}

}
