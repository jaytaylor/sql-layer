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

package com.foundationdb.server.store;

import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.rowdata.CorruptRowDataException;
import com.foundationdb.server.rowdata.RowData;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.HandleCache;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

public class RowDataValueCoder implements ValueDisplayer, ValueRenderer, HandleCache {
    final static int INITIAL_BUFFER_SIZE = 1024;

    private volatile int handle;

    RowDataValueCoder() {
    }

    @Override
    public void put(final Value value, final Object object, final CoderContext context) throws ConversionException {
        final RowData rowData = (RowData) object;
        final int start = rowData.getInnerStart();
        final int size = rowData.getInnerSize();
        value.ensureFit(size);
        final int at = value.getEncodedSize();
        System.arraycopy(rowData.getBytes(), start, value.getEncodedBytes(), at, size);
        value.setEncodedSize(at + size);
    }

    @Override
    public Object get(final Value value, final Class<?> clazz, final CoderContext context) throws ConversionException {
        final RowData rowData = new RowData(new byte[INITIAL_BUFFER_SIZE]);
        render(value, rowData, RowData.class, null);
        return rowData;
    }

    @Override
    public void render(final Value value, final Object target, final Class<?> clazz, final CoderContext context)
            throws ConversionException {
        final RowData rowData = (RowData) target;
        final int at = value.getCursor();
        final int end = value.getEncodedSize();
        final int size = end - at;

        final int rowDataSize = size + RowData.ENVELOPE_SIZE;
        final byte[] valueBytes = value.getEncodedBytes();
        byte[] rowDataBytes = rowData.getBytes();

        if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH || rowDataSize > RowData.MAXIMUM_RECORD_LENGTH) {
            throw new CorruptRowDataException("RowData is too short or too long: " + rowDataSize);
        }

        if (rowDataBytes == null || rowDataSize > rowDataBytes.length) {
            rowDataBytes = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
            rowData.reset(rowDataBytes);
        }

        //
        // Assemble the Row in a byte array
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
    public void display(final Value value, final StringBuilder target, final Class<?> clazz, final CoderContext context)
            throws ConversionException {
        final Object object = get(value, clazz, context);
        if (object instanceof RowData) {
            final RowData rowData = (RowData) object;
            target.append(rowData.toString());
        } else {
            target.append(object);
        }
    }

    @Override
    public synchronized void setHandle(final int handle) {
        if (this.handle != 0 && this.handle != handle) {
            throw new IllegalStateException("Attempt to change handle from " + this.handle + " to " + handle);
        }
        this.handle = handle;
    }

    @Override
    public int getHandle() {
        return this.handle;
    }
}
