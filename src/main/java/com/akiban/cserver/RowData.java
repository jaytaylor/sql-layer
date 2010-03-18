package com.akiban.cserver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
 *  +12: null-map (1 bit per schema-defined column)
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

	public final static int O_LENGTH_A = 0;

	public final static int O_SIGNATURE_A = 4;

	public final static int O_FIELD_COUNT = 6;

	public final static int O_ROW_DEF_ID = 8;

	public final static int O_NULL_MAP = 12;

	public final static int O_SIGNATURE_B = -6;

	public final static int O_LENGTH_B = -4;

	public final static int MINIMUM_RECORD_LENGTH = 18;

	public final static char SIGNATURE_A = (char) ('A' + ('B' << 8));

	public final static char SIGNATURE_B = (char) ('B' + ('A' << 8));

	public final static int ENVELOPE_SIZE = 12;

	public final static int LEFT_ENVELOPE_SIZE = 6;

	public final static int RIGHT_ENVELOPE_SIZE = 6;

	private final static SimpleDateFormat SDF_DATETIME = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:SS");

	private final static SimpleDateFormat SDF_TIME = new SimpleDateFormat(
			"HH:mm:SS");

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

	public void reset(final byte[] bytes) {
		this.bytes = bytes;
		reset(0, bytes.length);
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
	public boolean prepareRow(final int offset) throws CorruptRowDataException {
		if (offset == bufferEnd) {
			return false;
		}
		if (offset < 0 || offset + MINIMUM_RECORD_LENGTH > bufferEnd) {
			throw new CorruptRowDataException("Invalid offset: " + offset);
		} else {
			validateRow(offset);
			rowStart = offset;
			rowEnd = offset + CServerUtil.getInt(bytes, O_LENGTH_A + offset);
			return true;
		}
	}

	public void validateRow(final int offset) throws CorruptRowDataException {

		if (offset < 0 || offset + MINIMUM_RECORD_LENGTH > bufferEnd) {
			throw new CorruptRowDataException("Invalid offset: " + offset);
		} else {
			final int recordLength = CServerUtil.getInt(bytes, O_LENGTH_A
					+ offset);
			if (recordLength < 0 || recordLength + offset > bufferEnd) {
				throw new CorruptRowDataException("Invalid record length: "
						+ recordLength + " at offset: " + offset);
			}
			if (CServerUtil.getChar(bytes, O_SIGNATURE_A + offset) != SIGNATURE_A) {
				throw new CorruptRowDataException(
						"Invalid signature at offset: " + offset);
			}
			final int trailingLength = CServerUtil.getInt(bytes, offset
					+ recordLength + O_LENGTH_B);
			if (trailingLength != recordLength) {
				throw new CorruptRowDataException(
						"Invalid trailing record length " + trailingLength
								+ " in record at offset: " + offset);
			}
			if (CServerUtil.getChar(bytes, offset + recordLength
					+ O_SIGNATURE_B) != SIGNATURE_B) {
				throw new CorruptRowDataException(
						"Invalid signature at offset: " + offset);
			}
		}
	}

	public boolean elide(final byte[] bits, final int field, final int width) {
		// TODO - ignore for now
		return false;
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

	public int getBufferEnd() {
		return bufferEnd;
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

	public int getRowSize() {
		return rowEnd - rowStart;
	}

	public int getInnerStart() {
		return rowStart + O_FIELD_COUNT;
	}

	public int getInnerSize() {
		return rowEnd - rowStart + O_SIGNATURE_B - O_FIELD_COUNT;
	}

	public int getFieldCount() {
		return CServerUtil.getChar(bytes, rowStart + O_FIELD_COUNT);
	}

	public int getRowDefId() {
		return CServerUtil.getInt(bytes, rowStart + O_ROW_DEF_ID);
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
		return CServerUtil.getUnsignedIntegerByWidth(bytes, offset, width);
	}

	public String getStringValue(final int offset, final int width,
			final int declaredWidth) {
		if (offset < rowStart || offset + width >= rowEnd) {
			throw new IllegalArgumentException("Bad location: " + offset + ":"
					+ width);
		}
		return CServerUtil.decodeMySQLString(bytes, offset, width,
				declaredWidth);
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
		CServerUtil.putChar(bytes, offset + O_SIGNATURE_A, SIGNATURE_A);
		CServerUtil.putInt(bytes, offset + O_ROW_DEF_ID, rowDef.getRowDefId());
		CServerUtil.putChar(bytes, offset + O_FIELD_COUNT, fieldCount);
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
				final int width = fieldDef.getMaxWidth();
				long value = 0;
				if (object == null) {
					continue;
				}
				switch (fieldDef.getType()) {
				
				case U_TINYINT:
				case U_SMALLINT:
				case U_MEDIUMINT:
				case U_INT:
				case U_BIGINT:

				case TINYINT:
				case SMALLINT:
				case MEDIUMINT:
				case INT:
				case BIGINT:
					value = ((Number) object).longValue();
					break;
				case FLOAT:
					value = Float.floatToRawIntBits(((Number)object).floatValue());
					break;
				case DOUBLE:
					value = Double.doubleToRawLongBits(((Number)object).doubleValue());
					break;
				case DATETIME:
				case TIMESTAMP:
					final Date date;
					try {
						date = SDF_DATETIME.parse((String) object);
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
					if (fieldDef.getType() == FieldType.TIMESTAMP) {
						value = (int) (date.getTime() / 1000);
					} else {
						int hi = ((date.getYear() + 1900) * 10000)
								+ (date.getMonth() * 100) + date.getDate();
						int low = (date.getHours() * 10000)
								+ (date.getMinutes() * 100) + date.getSeconds();
						value = ((long) hi) << 32 + (long) low;
					}
					break;
				case TIME:
					final Date time;
					try {
						date = SDF_TIME.parse((String) object);
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
					value = date.getHours() * 3600 + date.getMinutes() * 60
							+ date.getSeconds();
					break;

				default:
					throw new UnsupportedOperationException(
							"Unable to encode type " + fieldDef);
				}
				switch (width) {
				case 1:
					CServerUtil.putByte(bytes, offset, (byte) value);
					break;
				case 2:
					CServerUtil.putShort(bytes, offset, (short) value);
					break;
				case 3:
					CServerUtil.putMediumInt(bytes, offset, (int) value);
					break;
				case 4:
					CServerUtil.putInt(bytes, offset, (int) value);
					break;
				case 8:
					CServerUtil.putLong(bytes, offset, value);
					break;
				default:
					throw new IllegalStateException("Width not supported");
				}
				offset += width;
			} else {
				final int overhead = fieldDef.getWidthOverhead();
				vmax += fieldDef.getMaxWidth() + overhead;
				if (object == null) {
					continue;
				}
				vlength += getBytes(object).length + overhead;
				final int width = CServerUtil.varwidth(vmax);
				switch (width) {
				case 0:
					break;
				case 1:
					CServerUtil.putByte(bytes, offset, (byte) vlength);
					break;
				case 2:
					CServerUtil.putChar(bytes, offset, (char) vlength);
					break;
				case 3:
					CServerUtil.putMediumInt(bytes, offset, (int) vlength);
					break;
				}
				offset += width;
			}
		}
		for (int index = 0; index < values.length; index++) {
			final FieldDef fieldDef = rowDef.getFieldDef(index);
			if (!fieldDef.isFixedWidth() && values[index] != null) {
				final byte[] b = getBytes(values[index]);
				switch (fieldDef.getWidthOverhead()) {
				case 0:
					break;
				case 1:
					CServerUtil.putByte(bytes, offset, (byte) b.length);
					offset += 1;
					break;
				case 2:
					CServerUtil.putChar(bytes, offset, (short) b.length);
					offset += 2;
					break;
				case 3:
					CServerUtil.putMediumInt(bytes, offset, (int) b.length);
					offset += 3;
					break;
				}
				CServerUtil.putBytes(bytes, offset, b);
				offset += b.length;
			}
		}
		CServerUtil.putChar(bytes, offset, SIGNATURE_B);
		offset += 6;
		final int length = offset - rowStart;
		CServerUtil.putInt(bytes, rowStart + O_LENGTH_A, length);
		CServerUtil.putInt(bytes, offset + O_LENGTH_B, length);
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
		return CServerUtil.dump(bytes, 0, bytes.length);
	}

	public String toString(final RowDefCache cache) {
		final StringBuilder sb = new StringBuilder();
		final RowDef rowDef = cache != null ? cache.getRowDef(getRowDefId())
				: null;
		try {
			if (rowDef == null) {
				sb.append("RowData?(rowDefId=");
				sb.append(getRowDefId());
				sb.append(": ");
				CServerUtil.hex(sb, bytes, rowStart, rowEnd - rowStart);
			} else {
				sb.append(rowDef.getTableName());
				for (int i = 0; i < getFieldCount(); i++) {
					final FieldDef fieldDef = rowDef.getFieldDef(i);
					final long location = rowDef.fieldLocation(this, i);
					sb.append(i == 0 ? "(" : ",");
					switch (fieldDef.getType()) {
					case TINYINT:
					case SMALLINT:
					case MEDIUMINT:
					case INT:
					case BIGINT: {
						sb.append(getIntegerValue((int) location,
								(int) (location >>> 32)));
						break;
					}
					case VARCHAR:
					case CHAR:
					case TINYTEXT:
					case TEXT:
					case MEDIUMTEXT:
					case LONGTEXT:
					case TINYBLOB:
					case BLOB:
					case MEDIUMBLOB:
					case LONGBLOB: {
						sb.append("\'");
						int start = (int) location;
						int size = (int) (location >>> 32);
						final int prefix = fieldDef.getWidthOverhead();
						final int length;
						switch (prefix) {
						case 0:
							length = 0;
							break;
						case 1:
							length = CServerUtil.getByte(bytes, start);
							break;
						case 2:
							length = CServerUtil.getChar(bytes, start);
							break;
						case 3:
							length = CServerUtil.getMediumInt(bytes, start);
						default:
							throw new Error("No such case");
						}
						start += prefix;
						size -= prefix;
						if (length != size) {
							sb.append("<length " + length
									+ " unequal to storage size " + size + ">");
							size = Math.min(length, size);
						}
						for (int j = 0; j < size; j++) {
							char c = (char) (bytes[j + start] & 0xFF);
							switch (c) {
							case '\\':
								sb.append("\\\\");
								break;
							case '\"':
								sb.append("\\\"");
								break;
							case '\'':
								sb.append("\\\'");
								break;
							case '\n':
								sb.append("\\n");
								break;
							case '\r':
								sb.append("\\r");
								break;
							case '\t':
								sb.append("\\t");
								break;
							default:
								sb.append(c);
							}
						}
						sb.append("\'");
						break;
					}
					case DATETIME: {
						final long dt = getIntegerValue((int) location, 8);
						final int hi = (int) (dt >>> 32);
						final int low = (int) dt;
						final int year = hi / 10000;
						final int month = (hi / 100) % 100;
						final int day = hi % 100;
						final int hour = low / 10000;
						final int minute = (low / 100) % 100;
						final int second = low % 100;
						final Date date = new Date(year - 1900, month, day,
								hour, minute, second);
						sb.append('\'');
						sb.append(SDF_DATETIME.format(date));
						sb.append('\'');
						break;
					}
					case TIMESTAMP: {
						final long time = ((long) getIntegerValue(
								(int) location, 4)) * 1000;
						final Date date = new Date(time);
						sb.append('\'');
						sb.append(SDF_DATETIME.format(date));
						sb.append('\'');
						break;
					}
					default: {
						sb.append("?" + rowDef.getFieldDef(i).getType() + "?");
					}
					}
				}
				sb.append(")");
			}
		} catch (Exception e) {
			int size = Math.min(getRowSize(), 64);
			if (size > 0 && rowStart >= 0) {
				sb.append(CServerUtil.dump(bytes, rowStart, size));
			}
			return sb.toString();
		}
		return sb.toString();
	}
}
