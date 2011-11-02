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

package com.akiban.server;

import com.akiban.ais.model.Column;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueTarget;
import com.akiban.util.ByteSource;
import com.persistit.Key;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PersistitKeyValueTarget implements ValueTarget {

    // PersistitKeyValueTarget interface

    public void attach(Key key) {
        this.key = key;
    }

    public PersistitKeyValueTarget expectingType(AkType type) {
        this.type = type;
        return this;
    }

    public PersistitKeyValueTarget expectingType(Column column) {
        return expectingType(column.getType().akType());
    }
    
    // ValueTarget interface

    @Override
    public void putNull() {
        checkState(AkType.NULL);
        key.append(null);
        invalidate();
    }

    @Override
    public void putDate(long value) {
        checkState(AkType.DATE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDateTime(long value) {
        checkState(AkType.DATETIME);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDecimal(BigDecimal value) {
        checkState(AkType.DECIMAL);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDouble(double value) {
        checkState(AkType.DOUBLE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putFloat(float value) {
        checkState(AkType.FLOAT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putInt(long value) {
        checkState(AkType.INT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putLong(long value) {
        checkState(AkType.LONG);
        key.append(value);
        invalidate();
    }

    @Override
    public void putString(String value) {
        checkState(AkType.VARCHAR);
        key.append(value);
        invalidate();
    }

    @Override
    public void putText(String value) {
        checkState(AkType.TEXT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putTime(long value) {
        checkState(AkType.TIME);
        key.append(value);
        invalidate();
    }

    @Override
    public void putTimestamp(long value) {
        checkState(AkType.TIMESTAMP);
        key.append(value);
        invalidate();
    }
    
    @Override
    public void putInterval(long value) {
        checkState(AkType.INTERVAL);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUBigInt(BigInteger value) {
        checkState(AkType.U_BIGINT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUDouble(double value) {
        checkState(AkType.U_DOUBLE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUFloat(float value) {
        checkState(AkType.U_FLOAT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUInt(long value) {
        checkState(AkType.U_INT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putVarBinary(ByteSource value) {
        checkState(AkType.VARBINARY);
        key().appendByteArray(value.byteArray(), value.byteArrayOffset(), value.byteArrayLength());
        invalidate();
    }

    @Override
    public void putYear(long value) {
        checkState(AkType.YEAR);
        key.append(value);
        invalidate();
    }

    @Override
    public void putBool(boolean value) {
        checkState(AkType.BOOL);
        key.append(value);
        invalidate();
    }

    @Override
    public AkType getConversionType() {
        return type;
    }

    // object interface

    @Override
    public String toString() {
        return key().toString();
    }

    // for use by this class

    protected final Key key() {
        return key;
    }
    
    // private methods

    private void checkState(AkType type) {
        ValueSourceHelper.checkType(this.type, type);
    }

    private void invalidate() {
        type = AkType.UNSUPPORTED;
    }

    // object state

    private Key key;
    private AkType type = AkType.UNSUPPORTED;
}
