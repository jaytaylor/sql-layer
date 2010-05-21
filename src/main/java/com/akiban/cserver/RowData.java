package com.akiban.cserver;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;

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

    // Arbitrary sanity bound on maximum size
    public final static int MAXIMUM_RECORD_LENGTH = 64 * 1024 * 1024;

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

    public RowData() {

    }

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

    public boolean isNull(final int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= getFieldCount()) {
            throw new IllegalArgumentException("No such field " + fieldIndex
                    + " in " + this);
        } else {
            return (getColumnMapByte(fieldIndex / 8) & (1 << (fieldIndex % 8))) != 0;
        }
    }

    public long getIntegerValue(final int offset, final int width) {
        if (offset < rowStart || offset + width >= rowEnd) {
            throw new IllegalArgumentException("Bad location: " + offset + ":"
                    + width);
        }
        return CServerUtil.getUnsignedIntegerByWidth(bytes, offset, width);
    }

    public String getStringValue(final int offset, final int width,
            final FieldDef fieldDef) {
        if (offset == 0 && width == 0) {
            return null;
        }
        if (offset < rowStart || offset + width >= rowEnd) {
            throw new IllegalArgumentException("Bad location: " + offset + ":"
                    + width);
        }
        return CServerUtil.decodeMySQLString(bytes, offset, width, fieldDef);
    }

    public void mergeFields(final RowDef rowDef, ArrayList<ByteBuffer> fields,
            int index, BitSet nullMap) {
        assert nullMap != null;
        final int fieldCount = rowDef.getFieldCount();
        assert fields.size() == rowDef.getFieldCount();
        int offset = rowStart;

        CServerUtil.putChar(bytes, offset + O_SIGNATURE_A, SIGNATURE_A);
        CServerUtil.putInt(bytes, offset + O_ROW_DEF_ID, rowDef.getRowDefId());
        CServerUtil.putChar(bytes, offset + O_FIELD_COUNT, fieldCount);

        offset = offset + O_NULL_MAP;
        int mapSize = ((fieldCount % 8) == 0 ? fieldCount : fieldCount + 1);        
        bytes[offset] = 0;
        
        for(int i=0; i < mapSize; i++) {
            if(nullMap.get(i)) { 
                bytes[offset] |= 1 << (i % 8); 
            }
            if((i+1) % 8 == 0) { 
                offset++;
                bytes[offset] = 0;
            }
        }
        offset++;

        for(int i = 0; i < fields.size(); i++) {
            if(!nullMap.get(i)) {
                int fieldSize = rowDef.getFieldDef(i).getMaxStorageSize();
                assert rowDef.getFieldDef(i).isFixedSize() == true;
                fields.get(i).get(bytes, offset, fieldSize);
                offset += fieldSize;
            }
        }

        CServerUtil.putChar(bytes, offset, SIGNATURE_B);
        offset += 6;
        final int length = offset - rowStart;
        CServerUtil.putInt(bytes, rowStart + O_LENGTH_A, length);
        CServerUtil.putInt(bytes, offset + O_LENGTH_B, length);
        rowEnd = offset;
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
            if (fieldDef.isFixedSize()) {
                if (object != null) {
                    offset += fieldDef.getEncoding().fromObject(fieldDef,
                            object, bytes, offset);
                }
            } else {
                vmax += fieldDef.getMaxStorageSize();
                if (object != null) {
                    vlength += fieldDef.getEncoding().widthFromObject(fieldDef,
                            object);
                    final int width = CServerUtil.varWidth(vmax);
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
        }
        for (int index = 0; index < values.length; index++) {
            Object object = values[index];
            final FieldDef fieldDef = rowDef.getFieldDef(index);
            if (object != null && !fieldDef.isFixedSize()) {
                offset += fieldDef.getEncoding().fromObject(fieldDef,
                        values[index], bytes, offset);
            }
        }
        CServerUtil.putChar(bytes, offset, SIGNATURE_B);
        offset += 6;
        final int length = offset - rowStart;
        CServerUtil.putInt(bytes, rowStart + O_LENGTH_A, length);
        CServerUtil.putInt(bytes, offset + O_LENGTH_B, length);
        rowEnd = offset;
    }

    /**
     * Debug-only: returns a hex-dump of the backing buffer.
     */
    @Override
    public String toString() {
        return CServerUtil.dump(bytes, rowStart, rowEnd - rowStart);
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
                    sb.append(i == 0 ? "(" : ",");
                    final long location = fieldDef.getRowDef().fieldLocation(
                            this, fieldDef.getFieldIndex());
                    if (location == 0) {
                        sb.append("null");
                    } else {
                        fieldDef.getEncoding().toString(fieldDef, this, sb,
                                Quote.SINGLE_QUOTE);
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

    public String explain() {

        final StringBuilder sb = new StringBuilder();
        return sb.toString();
    }
}
