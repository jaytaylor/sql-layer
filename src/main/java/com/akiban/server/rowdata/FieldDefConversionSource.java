/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.rowdata;

import com.akiban.server.types.ConversionSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public final class FieldDefConversionSource implements ConversionSource {

    // FieldDefConversionSource interface

    public void bind(FieldDef fieldDef, RowData rowData) {
        this.fieldDef = fieldDef;
        this.rowData = rowData;
    }

    // ConversionSource interface

    @Override
    public boolean isNull() {
        return (rowData.isNull(fieldDef.getFieldIndex()));
    }

    @Override
    public BigDecimal getDecimal() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public BigInteger getUBigInt() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public ByteBuffer getVarBinary() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public double getUDouble() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public float getUFloat() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getDate() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getDateTime() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getInt() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getLong() {
        return extractLong(fieldDef, rowData);
    }

    @Override
    public long getTime() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getUInt() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long getYear() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public String getString() {
        final long location = getOffsetAndWidth(fieldDef, rowData);
        return location == 0
                ? null
                : rowData.getStringValue((int) location, (int) (location >>> 32), fieldDef);
    }

    @Override
    public String getText() {
        throw new UnsupportedOperationException(); // TODO
    }

    private FieldDef fieldDef;
    private RowData rowData;

    // for use within this class
    // Stolen from the Encoding classes

    private static long getOffsetAndWidth(FieldDef fieldDef, RowData rowData) {
        return fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
    }

    private static long extractLong(FieldDef fieldDef, RowData rowData) {
        long offsetAndWidth = getOffsetAndWidth(fieldDef, rowData);
        assert offsetAndWidth != 0 : fieldDef + " null for " + rowData;
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        return rowData.getIntegerValue(offset, width);
    }
}
