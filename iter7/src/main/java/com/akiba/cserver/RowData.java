package com.akiba.cserver;

/**
 * Represent one or more rows of table data.  The backing store is a byte array
 * supplied in the constructor. The {@link #RowData(byte[], int, int)}
 * constructor allows use of a partially filled reusable buffer.
 * 
 * This class provides methods for both interpreting and constructing
 * row data structures in the byte array. After a call to {@link #reset(int, int)}
 * the index will point to the first field of the first row represented in
 * the buffer.
 * 
 *  +0: record length (int)
 *  +4: signature bytes, e.g., 'AB'
 *  +6: field count (short)
 *  +8: tvHandle (int)
 * +12: column-map (1 bit per schema-defined column)
 *  +M: fixed-length field
 *  +N: fixed-length field
 *  ...
 *  +Q: variable-length field
 *  +R: variable-length field
 *  ...
 * +Z-6: signature bytes: e.g., 'BA'
 * +Z-4: record length
 *
 * @author peter
 */
public class RowData {

	private final static int O_LENGTH_A = 0;

	private final static int O_SIGNATURE_A = 4;

	private final static int O_FIELD_COUNT = 6;

	private final static int O_TVHANDLE = 8;

	private final static int O_COLUMN_MAP = 12;
	
	private final static int O_DATA = 16;
	
	private final static int O_SIGNATURE_B = -6;

	private final static int O_LENGTH_B = -4;
	
	private final static int MINIMUM_RECORD_LENGTH = 18;
	
	private final static char SIGNATURE_A = (char)('A' + ('B' << 8)); 
	
	private final static char SIGNATURE_B = (char)('B' + ('A' << 8));
	
	private byte[] bytes;
	
	private int bufferStart;
	
	private int bufferEnd;
	
	private int rowStart;
	
	private int rowEnd;
	
	private int fieldCount;
	
	private int tvHandle;
	
	public RowData(final byte[] bytes) {
		this.bytes = bytes;
		reset(0, bytes.length);
	}
	
	public RowData(final byte[] bytes, final int offset, final int length) {
		this.bytes = bytes;
		reset(offset, length);
	}

	public void reset(final int offset, final int length) {
		this.bufferStart = offset;
		this.bufferEnd = offset + length;
		prepareRow(offset);
	}
	
	/**
	 * Interpret the length and signature fixed fields of the row at
	 * the specified offset.  This method sets the {@link #rowStart},
	 * {@link #rowEnd} and {@link #tvHandle} fields so that subsequent calls
	 * to interpret fields are supported.
	 * 
	 * @param offset byte offset to record start within the buffer
	 */
	public boolean prepareRow(final int offset) {
		if (offset == bufferEnd) {
			return false;
		}
		if (offset < 0 || offset + MINIMUM_RECORD_LENGTH > bufferEnd) {
			throw new CorruptRowDataException("Invalid offset: " + offset);
		} else {
			final int recordLength = Util.getInt(bytes, O_LENGTH_A + offset);
			if (recordLength < 0 || recordLength + offset > bufferEnd) {
				throw new CorruptRowDataException("Invalid record length: " + recordLength + " at offset: " + offset);
			}
			if (Util.getChar(bytes, O_SIGNATURE_A) != SIGNATURE_A) {
				throw new CorruptRowDataException("Invalid signature at offset: " + offset);
			}
			final int trailingLength = Util.getInt(bytes, offset + recordLength + O_LENGTH_B);
			if (trailingLength != recordLength) {
				throw new CorruptRowDataException("Invalid trailing record length " + 
						trailingLength + " in record at offset: " + offset);
			}
			if (Util.getChar(bytes, offset + recordLength + O_SIGNATURE_B) != SIGNATURE_B) {
				throw new CorruptRowDataException("Invalid signature at offset: " + offset);
			}
			rowStart = offset;
			rowEnd = offset + recordLength;
			fieldCount = Util.getChar(bytes, offset + O_FIELD_COUNT);
			tvHandle = Util.getByte(bytes, offset + O_TVHANDLE);
			return true;
		}
	}
	
	public boolean nextRow() {
		if (rowEnd < bufferEnd) {
			return prepareRow(rowEnd);
		} else {
			return false;
		}
	}

	public int getBufferStart() {
		return bufferStart;
	}

	public int getBufferLength() {
		return bufferEnd - bufferStart;
	}

	public int getRowStart() {
		return rowStart;
	}
	
	public int getRowStartData() {
		return rowStart + O_DATA;
	}

	public int getRowEnd() {
		return rowEnd;
	}
	
	public int getFieldCount() {
		return fieldCount;
	}
	
	public int getTvHandle() {
		return tvHandle;
	}

	public byte[] getBytes() {
		return bytes;
	}
	
	public int getColumnMapByte(final int offset) {
		return bytes[offset + rowStart + O_COLUMN_MAP] & 0xFF;
	}

	public int getVLen(final int offset) {
		int value = Util.getByte(bytes, offset);
		if (value > 0x7F) {
			value  = (value & 0x7f) | (Util.getByte(bytes, offset + 1) << 7);
			if (value > 0x3FFF) {
				value = (value & 0x3FFF) | (Util.getByte(bytes, offset + 2) << 14);
			}
		}
		if (value < 0 || value > 0x1FFFFF) {
			throw new IllegalArgumentException("VLen " + value + " is invalid");
		}
		return value;
	}
	
	public void putVLen(final int offset, final int value) {
		if (value < 0 || value > 0x1FFFFF) {
			throw new IllegalArgumentException("VLen " + value + " is invalid");
		}
		if (value > 0x7F) {
			bytes[offset] = (byte)((value & 0x7F) | 0x80);
			if (value > 0x3FFF) {
				bytes[offset + 1] = (byte)((value >>> 7 & 0x7F) | 0x80);
				bytes[offset + 2] = (byte)((value >>> 14 & 0x7F));
			} else {
				bytes[offset + 1] = (byte)((value >>> 7 & 0x7F));
			}
		} else {
			bytes[offset] = (byte)value;
		}
	}
	
	public int vlenLength(final int length) {
		if (length < 0 || length > 0x1FFFFF) {
			throw new IllegalArgumentException("VLen format cannot encode value: " + length);
		}
		return length <= 0x7F ? 1 : length <= 0x3FFF ? 2 : 3;
	}
	
}
