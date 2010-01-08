package com.akiba.cserver;

/**
 * Represent one or more rows of table data. The backing store is a byte array
 * supplied in the constructor. The {@link #RowData(byte[], int, int)}
 * constructor allows use of a partially filled reusable buffer.
 * 
 * This class provides methods for both interpreting and constructing row data
 * structures in the byte array. After a call to {@link #reset(int, int)} the
 * index will point to the first field of the first row represented in the
 * buffer.
 * 
 * <pre>
 *   +0: record length (Z) 
 *   +4: signature bytes, e.g., 'AB' 
 *   +6: field count (short) 
 *   +8: rowDefId (int) 
 *  +12: column-map (1 bit per schema-defined column)
 *   +M: fixed-length field 
 *   +N: fixed-length field ... 
 *   +Q: variable-length field
 *   +R: variable-length field ... 
 *   +Z-6: signature bytes: e.g., 'BA' 
 *   +Z-4: record length (Z)
 * </pre>
 * 
 * @author peter
 */
public class RowData {

	private final static int O_LENGTH_A = 0;

	private final static int O_SIGNATURE_A = 4;

	private final static int O_FIELD_COUNT = 6;

	private final static int O_ROW_DEF_ID = 8;

	private final static int O_NULL_MAP = 12;

	private final static int O_SIGNATURE_B = -6;

	private final static int O_LENGTH_B = -4;

	private final static int MINIMUM_RECORD_LENGTH = 18;

	private final static char SIGNATURE_A = (char) ('A' + ('B' << 8));

	private final static char SIGNATURE_B = (char) ('B' + ('A' << 8));

	private byte[] bytes;

	private int bufferStart;

	private int bufferEnd;

	private int rowStart;

	private int rowEnd;

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
		rowStart = rowEnd = bufferStart;
	}

	/**
	 * Interpret the length and signature fixed fields of the row at the
	 * specified offset. This method sets the {@link #rowStart}, {@link #rowEnd}
	 * and {@link #rowDefId} fields so that subsequent calls to interpret fields
	 * are supported.
	 * 
	 * @param offset
	 *            byte offset to record start within the buffer
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
				throw new CorruptRowDataException("Invalid record length: "
						+ recordLength + " at offset: " + offset);
			}
			if (Util.getChar(bytes, O_SIGNATURE_A + offset) != SIGNATURE_A) {
				throw new CorruptRowDataException(
						"Invalid signature at offset: " + offset);
			}
			final int trailingLength = Util.getInt(bytes, offset + recordLength
					+ O_LENGTH_B);
			if (trailingLength != recordLength) {
				throw new CorruptRowDataException(
						"Invalid trailing record length " + trailingLength
								+ " in record at offset: " + offset);
			}
			if (Util.getChar(bytes, offset + recordLength + O_SIGNATURE_B) != SIGNATURE_B) {
				throw new CorruptRowDataException(
						"Invalid signature at offset: " + offset);
			}
			rowStart = offset;
			rowEnd = offset + recordLength;
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
		return rowStart + O_NULL_MAP + (getFieldCount() + 7) / 8;
	}

	public int getRowEnd() {
		return rowEnd;
	}
	
	public int getInnerStart() {
		return rowStart + O_FIELD_COUNT;
	}
	
	public int getInnerSize() {
		return rowEnd - rowStart + O_SIGNATURE_B - O_FIELD_COUNT;
	}

	public int getFieldCount() {
		return Util.getChar(bytes, rowStart + O_FIELD_COUNT);
	}

	public int getRowDefId() {
		return Util.getInt(bytes, rowStart + O_ROW_DEF_ID);
	}

	public byte[] getBytes() {
		return bytes;
	}

	public int getColumnMapByte(final int offset) {
		return bytes[offset + rowStart + O_NULL_MAP] & 0xFF;
	}

	public long getIntegerValue(final int offset, final int width) {
		if (offset < rowStart || offset + width >= rowEnd) {
			throw new IllegalArgumentException("Bad location: " + offset + ":"
					+ width);
		}
		return Util.getUnsignedIntegerByWidth(bytes, offset, width);
	}

	/**
	 * For debugging only, poke some Java values supplied in the values array
	 * into a RowData instance. The conversions are very approximate!
	 * 
	 * @param rowDef
	 * @param values
	 */
	public void createRow(final RowDef rowDef, final Object[] values) {
		final int fieldCount = rowDef.getFieldCount();
		if (values.length > rowDef.getFieldCount()) {
			throw new IllegalArgumentException("Too many values.");
		}
		int offset = rowStart;
		Util.putChar(bytes, offset + O_SIGNATURE_A, SIGNATURE_A);
		Util.putInt(bytes, offset + O_ROW_DEF_ID, rowDef.getRowDefId());
		Util.putChar(bytes, offset + O_FIELD_COUNT, fieldCount);
		offset = offset + O_NULL_MAP;
		for (int index = 0; index < fieldCount; index += 8) {
			int b = 0;
			for (int j = index; j < index + 8 && j < fieldCount; j++) {
				if (j >= values.length || values[j] == null) {
					b |= (1 << j - index);
				}
			}
			bytes[offset++] = (byte) b;
		}
		int vlength = 0;
		int vmax = 0;
		for (int index = 0; index < values.length; index++) {
			Object object = values[index];
			FieldDef fieldDef = rowDef.getFieldDef(index);
			if (fieldDef.isFixedWidth()) {
				if (object == null) {
					continue;
				}
				final int width = fieldDef.getMaxWidth();
				long value = ((Number) object).longValue();
				switch (width) {
				case 1:
					Util.putByte(bytes, offset, (byte) value);
					break;
				case 2:
					Util.putShort(bytes, offset, (short) value);
					break;
				case 3:
					Util.putMediumInt(bytes, offset, (int) value);
					break;
				case 4:
					Util.putInt(bytes, offset, (int) value);
					break;
				case 8:
					Util.putLong(bytes, offset, value);
					break;
				default:
					throw new IllegalStateException("Width not supported");
				}
				offset += width;
			} else {
				vmax += fieldDef.getMaxWidth();
				if (object == null) {
					continue;
				}
				int width = vmax == 0 ? 0 : vmax < 256 ? 1 : vmax < 65536 ? 2
						: 3;
				vlength += getBytes(object).length;
				switch (width) {
				case 0:
					break;
				case 1:
					Util.putByte(bytes, offset, (byte) vlength);
					break;
				case 2:
					Util.putShort(bytes, offset, (short) vlength);
					break;
				case 3:
					Util.putMediumInt(bytes, offset, (int) vlength);
					break;
				}
				offset += width;
			}
		}
		for (int index = 0; index < values.length; index++) {
			final FieldDef fieldDef = rowDef.getFieldDef(index);
			if (!fieldDef.isFixedWidth()) {
				final byte[] b = getBytes(values[index]);
				Util.putBytes(bytes, offset, b);
				offset += b.length;
			}
		}
		Util.putChar(bytes, offset, SIGNATURE_B);
		offset += 6;
		final int length = offset - rowStart;
		Util.putInt(bytes, rowStart + O_LENGTH_A, length);
		Util.putInt(bytes, offset + O_LENGTH_B, length);
		rowEnd = offset;
	}

	private byte[] getBytes(final Object object) {
		if (object == null) {
			return new byte[0];
		} else if (object instanceof byte[]) {
			return (byte[]) object;
		} else {
			return ((String) object).getBytes();
		}
	}

	/**
	 * Debug-only: returns a hex-dump of the backing buffer.
	 */
	@Override
	public String toString() {
		return Util.dump(bytes, 0, bytes.length);
	}

}
