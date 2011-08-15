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

import com.akiban.server.AkServerUtil;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionTarget;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class RowDataConversionTarget extends RowDataConversionBase implements ConversionTarget {

    public int offset() {
        return offset;
    }

    public void offset(int offset) {
        ArgumentValidation.isGTE("offset", offset, 0);
        this.offset = offset;
    }

    public void bumpOffset(int delta) {
        ArgumentValidation.isGT("offset delta", delta, 0);
        offset += delta;
    }

    // ConversionTarget interface

    @Override
    public void putNull() {
        checkState();
        // nothing to do; the null bitmap will have been set externally
    }

    @Override
    public void putDate(long value) {
        checkState();
        bumpOffset(encodeLong(value));
    }

    @Override
    public void putDateTime(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public void putDecimal(BigDecimal value) {
        checkState();
        bumpOffset(ConversionHelperBigDecimal.fromObject(fieldDef(), value, bytes(), offset()));
    }

    @Override
    public void putDouble(double value) {
        checkState();
        bumpOffset( encodeLong(Double.doubleToLongBits(value)) );
    }

    @Override
    public void putFloat(float value) {
        checkState();
        bumpOffset( encodeInt(Float.floatToIntBits(value)) );
    }

    @Override
    public void putInt(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public void putLong(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public void putString(String value) {
        checkState();
        bumpOffset( ConversionHelper.encodeString(value, bytes(), offset(), fieldDef()) );
    }

    @Override
    public void putText(String value) {
        checkState();
        bumpOffset( ConversionHelper.encodeString(value, bytes(), offset(), fieldDef()) );
    }

    @Override
    public void putTime(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public void putTimestamp(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public void putUBigInt(BigInteger value) {
        checkState();
        assert encodableAsLong(value) : value;
        long asLong = value.longValue();
        int width = fieldDef().getMaxStorageSize();
        bumpOffset( AkServerUtil.putIntegerByWidth(bytes(), offset(), width, asLong));
    }

    @Override
    public void putUDouble(double value) {
        checkState();
        // TODO call to Math.max lifted from UDoubleEncoder.fromObject. Probably doesn't belong here.
        int longBits = Math.max(encodeLong(Double.doubleToLongBits(value)), 0);
        bumpOffset(longBits);
    }

    @Override
    public void putUFloat(float value) {
        checkState();
        // TODO call to Math.max lifted from UFloatEncoder.fromObject. Probably doesn't belong here.
        int intBits = Math.max(encodeInt(Float.floatToIntBits(value)), 0);
        bumpOffset(intBits);
    }

    @Override
    public void putUInt(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public void putVarBinary(ByteSource value) {
        checkState();
        bumpOffset(
                ConversionHelper.putByteArray(
                        value.byteArray(), value.byteArrayOffset(), value.byteArrayLength(),
                        bytes(), offset(), fieldDef())
        );
    }

    @Override
    public void putYear(long value) {
        checkState();
        bumpOffset( encodeLong(value) );
    }

    @Override
    public AkType getConversionType() {
        checkState();
        return fieldDef().getType().akType();
    }

    // private methods

    private byte[] bytes() {
        return rowData().getBytes();
    }

    private void checkState() {
        boolean badOffset = offset < 0;
        boolean nullRowData = rowData() == null;
        boolean nullFieldDef = fieldDef() == null;
        if (badOffset || nullRowData || nullFieldDef) {
            StringBuilder sb = new StringBuilder("bad state: ");
            if (badOffset) {
                sb.append("offset=").append(offset).append(", ");
            }
            if (nullRowData) {
                sb.append("rowData is null, ");
            }
            if (nullFieldDef) {
                sb.append("fieldDef is null, ");
            }
            sb.setLength( sb.length() - 2 );
            throw new IllegalStateException(sb.toString());
        }
    }

    private int encodeInt(int value) {
        assert INT_STORAGE_SIZE == fieldDef().getMaxStorageSize() : fieldDef().getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(bytes(), offset(), INT_STORAGE_SIZE, value);
    }

    private int encodeLong(long value) {
        int width = fieldDef().getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(bytes(), offset(), width, value);
    }

    private boolean encodableAsLong(BigInteger value) {
        return value.compareTo(MAX_BIGINT) <= 0;
    }

    // object state

    private int offset = -1;

    // consts

    private static final int INT_STORAGE_SIZE = 4;

    /**
     * We want to encode BigInteger as long, so we require it to be smaller than (2^64) + 1
     */
    private static final BigInteger MAX_BIGINT = BigInteger.valueOf(2).pow(Long.SIZE).add(BigInteger.ONE);
}
