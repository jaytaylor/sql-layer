package com.akiban.server.store;

import com.akiban.server.AkServerUtil;
import com.akiban.server.error.RowDataCorruptionException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

public class RowDataValueCoder implements ValueDisplayer, ValueRenderer {
    final static int INITIAL_BUFFER_SIZE = 1024;

    final PersistitStore store;

    RowDataValueCoder(final PersistitStore store) {
        this.store = store;
    }

    @Override
    public void put(Value value, Object object, CoderContext context) throws ConversionException {
        final RowData rowData = (RowData) object;
        final int start = rowData.getInnerStart();
        final int size = rowData.getInnerSize();
        value.ensureFit(size);
        final int at = value.getEncodedSize();
        System.arraycopy(rowData.getBytes(), start, value.getEncodedBytes(), at, size);
        value.setEncodedSize(at + size);
    }

    @Override
    public Object get(Value value, Class<?> clazz, CoderContext context) throws ConversionException {
        final RowData rowData = new RowData(new byte[INITIAL_BUFFER_SIZE]);
        render(value, rowData, RowData.class, null);
        return rowData;
    }

    @Override
    public void render(Value value, Object target, Class<?> clazz, CoderContext context) throws ConversionException {
        final RowData rowData = (RowData) target;
        // TODO this needs to be a more specific exception
        final int at = value.getCursor();
        final int end = value.getEncodedSize();
        final int size = end - at;

        final int rowDataSize = size + RowData.ENVELOPE_SIZE;
        final byte[] valueBytes = value.getEncodedBytes();
        byte[] rowDataBytes = rowData.getBytes();

        if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH || rowDataSize > RowData.MAXIMUM_RECORD_LENGTH) {
            throw new RowDataCorruptionException("RowData is too short or too long: " + rowDataSize);
        }

        if (rowDataSize > rowDataBytes.length) {
            rowDataBytes = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
            rowData.reset(rowDataBytes);
        }

        //
        // Assemble the Row in a byte array to allow column
        // elision
        //
        AkServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_A, rowDataSize);
        AkServerUtil.putShort(rowDataBytes, RowData.O_SIGNATURE_A, RowData.SIGNATURE_A);
        System.arraycopy(valueBytes, at, rowDataBytes, RowData.O_FIELD_COUNT, size);
        AkServerUtil.putShort(rowDataBytes, RowData.O_SIGNATURE_B + rowDataSize, RowData.SIGNATURE_B);
        AkServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_B + rowDataSize, rowDataSize);
        rowData.prepareRow(0);

        value.setCursor(end);

    }

    @Override
    public void display(Value value, StringBuilder target, Class<?> clazz, CoderContext context) throws ConversionException {
        Object object = get(value, clazz, context);
        if (object instanceof RowData) {
            final RowData rowData = (RowData) object;
            final int rowDefId = rowData.getRowDefId();
            final RowDef rowDef = store.getRowDefCache().getRowDef(rowDefId);
            target.append(rowData.toString(rowDef));
        } else {
            target.append(object);
        }
    }

}
