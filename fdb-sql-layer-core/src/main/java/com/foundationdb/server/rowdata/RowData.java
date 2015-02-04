/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.rowdata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.rowdata.encoding.EncodingException;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.util.AkibanAppender;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;

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
    public final static int MAXIMUM_RECORD_LENGTH = 8 * 1024 * 1024;

    public final static char SIGNATURE_A = (char) ('A' + ('B' << 8));

    public final static char SIGNATURE_B = (char) ('B' + ('A' << 8));

    public final static int ENVELOPE_SIZE = 12;

    public final static int LEFT_ENVELOPE_SIZE = 6;

    public final static int RIGHT_ENVELOPE_SIZE = 6;

    public final static int CREATE_ROW_INITIAL_SIZE = 500;

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

    public void reset(final byte[] bytes, final int offset, final int length) {
        this.bytes = bytes;
        reset(offset, length);
    }

    /**
     * Interpret the length and signature fixed fields of the row at the
     * specified offset. This method sets the {@link #rowStart} and {@link #rowEnd}
     * fields so that subsequent calls to interpret fields are supported.
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
            rowEnd = offset + AkServerUtil.getInt(bytes, O_LENGTH_A + offset);
            return true;
        }
    }

    public void validateRow(final int offset) throws CorruptRowDataException {

        if (offset < 0 || offset + MINIMUM_RECORD_LENGTH > bufferEnd) {
            throw new CorruptRowDataException("Invalid offset: " + offset);
        } else {
            final int recordLength = AkServerUtil.getInt(bytes, O_LENGTH_A + offset);
            if (recordLength < 0 || recordLength + offset > bufferEnd) {
                throw new CorruptRowDataException("Invalid record length: "
                        + recordLength + " at offset: " + offset);
            }
            if (AkServerUtil.getUShort(bytes, O_SIGNATURE_A + offset) != SIGNATURE_A) {
                throw new CorruptRowDataException(
                        "Invalid signature at offset: " + offset);
            }
            final int trailingLength = AkServerUtil.getInt(bytes, offset + recordLength + O_LENGTH_B);
            if (trailingLength != recordLength) {
                throw new CorruptRowDataException(
                        "Invalid trailing record length " + trailingLength
                                + " in record at offset: " + offset);
            }
            if (AkServerUtil.getUShort(bytes, offset + recordLength + O_SIGNATURE_B) != SIGNATURE_B) {
                throw new CorruptRowDataException(
                        "Invalid signature at offset: " + offset);
            }
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
        return AkServerUtil.getUShort(bytes, rowStart + O_FIELD_COUNT);
    }

    public int getRowDefId() {
        return AkServerUtil.getInt(bytes, rowStart + O_ROW_DEF_ID);
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
        checkOffsetAndWidth(offset, width);
        return AkServerUtil.getSignedIntegerByWidth(bytes, offset, width);
    }

    public long getUnsignedIntegerValue(final int offset, final int width) {
        checkOffsetAndWidth(offset, width);
        return AkServerUtil.getUnsignedIntegerByWidth(bytes, offset, width);
    }

    public String getStringValue(final int offset, final int width, final FieldDef fieldDef) {
        if (offset == 0 && width == 0) {
            return null;
        }
        checkOffsetAndWidth(offset, width);
        return AkServerUtil.decodeMySQLString(bytes, offset, width, fieldDef);
    }

    private void checkOffsetAndWidth(int offset, int width) {
        if (offset < rowStart || offset + width >= rowEnd) {
            throw new IllegalArgumentException(String.format("Bad location: {offset=%d width=%d start=%d end=%d}",
                    offset, width, rowStart, rowEnd));
        }
    }

    public RowData copy()
    {
        byte[] copyBytes = new byte[rowEnd - rowStart];
        System.arraycopy(bytes, rowStart, copyBytes, 0, rowEnd - rowStart);
        RowData copy = new RowData(copyBytes);
        copy.prepareRow(0);
        return copy;
    }

    public void createRow(final RowDef rowDef, final Object[] values, boolean growBuffer)
    {
        if (growBuffer && !(bufferStart == 0 && bufferEnd == bytes.length)) {
            // This RowData is embedded in a larger buffer. Can't grow it safely.
            throw new CannotGrowBufferException();
        }
        RuntimeException exception = null;
        do {
            try {
                exception = null;
                createRow(rowDef, values);
            } catch (ArrayIndexOutOfBoundsException e) {
                exception = e;
            } catch (EncodingException e) {
                if (e.getCause() instanceof ArrayIndexOutOfBoundsException) {
                    exception = e;
                } else {
                    throw e;
                }
            }
            if (exception != null && growBuffer) {
                int newSize = bytes.length == 0 ? CREATE_ROW_INITIAL_SIZE : bytes.length * 2;
                reset(new byte[newSize]);
            }
        } while (growBuffer && exception != null);
        if (exception != null) {
            throw exception;
        }
    }

    public void createRow(final RowDef rowDef, final Object[] values)
    {
        if (values.length > rowDef.getFieldCount()) {
            throw new IllegalArgumentException("Too many values.");
        }
        // Serialize spatial objects
        Object[] valuesWithSpatialObjectsSerialized = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof JTSSpatialObject) {
                UnderlyingType underlyingType = TInstance.underlyingType(rowDef.getFieldDef(i).column().getType());
                switch (underlyingType) {
                    case BYTES:
                        valuesWithSpatialObjectsSerialized[i] = Spatial.serializeWKB((JTSSpatialObject) values[i]);
                        break;
                    case STRING:
                        valuesWithSpatialObjectsSerialized[i] = Spatial.serializeWKT((JTSSpatialObject) values[i]);
                        break;
                    default:
                        assert false : rowDef.getFieldDef(i);
                }
            } else {
                valuesWithSpatialObjectsSerialized[i] = values[i];
            }
        }
        //
        RowDataBuilder builder = new RowDataBuilder(rowDef, this);
        builder.startAllocations();
        for (int i=0; i < values.length; ++i) {
            FieldDef fieldDef = rowDef.getFieldDef(i);
            builder.allocate(fieldDef, valuesWithSpatialObjectsSerialized[i]);
        }
        builder.startPuts();
        for (Object object : valuesWithSpatialObjectsSerialized) {
            builder.putObject(object);
        }
        rowEnd = builder.finalOffset();
    }

    public void updateNonNullLong(FieldDef fieldDef, long rowId)
    {
        // Offset is in low 32 bits of fieldLocation return value
        int offset = (int) fieldDef.getRowDef().fieldLocation(this, fieldDef.getFieldIndex());
        AkServerUtil.putLong(bytes, offset, rowId);
    }

    @Override
    public String toString() {
        return toString(RowDefBuilder.LATEST_FOR_DEBUGGING);
    }

    public String toString(AkibanInformationSchema ais) {
        if (ais == null) {
            return toStringWithoutRowDef("No AIS");
        }
        int rowDefID = getRowDefId();
        Table table = ais.getTable(rowDefID);
        if(table == null) {
            return toStringWithoutRowDef("Unknown RowDefID(" + rowDefID + ")");
        }
        return toString(table.rowDef());
    }

    public String toString(final RowDef rowDef)
    {
        if (rowDef == null) {
            return toStringWithoutRowDef("No RowDef provided");
        }
        final AkibanAppender sb = AkibanAppender.of(new StringBuilder());
        RowDataValueSource source = new RowDataValueSource();
        try {
            sb.append(rowDef.table().getName().getTableName());
            for (int i = 0; i < getFieldCount(); i++) {
                final FieldDef fieldDef = rowDef.getFieldDef(i);
                sb.append(i == 0 ? '(' : ',');
                final long location = fieldDef.getRowDef().fieldLocation(
                    this, fieldDef.getFieldIndex());
                if (location == 0) {
                    // sb.append("null");
                } else {
                    source.bind(fieldDef, this);
                    fieldDef.column().getType().format(source, sb);
                }
            }
            sb.append(')');
        } catch (Exception e) {
            int size = Math.min(getRowSize(), 64);
            if (size > 0 && rowStart >= 0) {
                sb.append(AkServerUtil.dump(bytes, rowStart, size));
            }
            return sb.toString();
        }
        return sb.toString();
    }

    /** Returns a hex-dump of the backing buffer. */
    public String toStringWithoutRowDef(String missingRowDefExplanation) {
        final AkibanAppender sb = AkibanAppender.of(new StringBuilder());
        try {
            sb.append("RowData[");
            sb.append(missingRowDefExplanation);
            sb.append("]?(rowDefId=");
            sb.append(getRowDefId());
            sb.append(": ");
            AkServerUtil.hex(sb, bytes, rowStart, rowEnd - rowStart);
        } catch (Exception e) {
            int size = Math.min(getRowSize(), 64);
            if (size > 0 && rowStart >= 0) {
                sb.append(AkServerUtil.dump(bytes, rowStart, size));
            }
            return sb.toString();
        }
        return sb.toString();
    }
}
