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
import com.akiban.server.types.ConversionTarget;
import com.akiban.util.ByteSource;
import com.persistit.Key;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PersistitKeyConversionTarget implements ConversionTarget {

    // PersistitKeyConversionTarget interface

    public void attach(Key key) {
        this.key = key;
    }

    public PersistitKeyConversionTarget expectingType(AkType type) {
        this.type = type;
        return this;
    }

    public PersistitKeyConversionTarget expectingType(Column column) {
        return expectingType(column.getType().akType());
    }
    
    // ConversionTarget interface

    @Override
    public void putNull() {
        key.append(null);
        invalidate();
    }

    @Override
    public void putDate(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putDateTime(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putDecimal(BigDecimal value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putDouble(double value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putFloat(float value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putInt(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putLong(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putString(String value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putText(String value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putTime(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putTimestamp(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putUBigInt(BigInteger value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putUDouble(double value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putUFloat(float value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putUInt(long value) {
        checkState();
        key.append(value);
        invalidate();
    }

    @Override
    public void putVarBinary(ByteSource value) {
        key().appendByteArray(value.byteArray(), value.byteArrayOffset(), value.byteArrayLength());
    }

    @Override
    public void putYear(long value) {
        checkState();
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
    
    private void checkState() {
        if (type == AkType.UNSUPPORTED) {
            throw new IllegalStateException("target AkType not set");
        }
    }

    private void invalidate() {
        type = AkType.UNSUPPORTED;
    }

    // object state

    private Key key;
    private AkType type = AkType.UNSUPPORTED;
}
