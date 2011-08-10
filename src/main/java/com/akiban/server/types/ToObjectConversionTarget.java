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

package com.akiban.server.types;

import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class ToObjectConversionTarget implements ConversionTarget {
    
    // ToObjectConversionTarget interface

    /**
     * Convenience method for extracting an Object from a conversion source.
     * @param source the incoming source
     * @return the converted Object
     */
    public Object convertFromSource(ConversionSource source) {
        expectType(source.getConversionType());
        return Converters.convert(source, this).lastConvertedValue();
    }

    public ToObjectConversionTarget expectType(AkType type) {
        this.akType = type;
        putPending = true;
        return this;
    }
    
    public Object lastConvertedValue() {
        if (putPending) {
            throw new IllegalStateException("put is pending for type " + akType);
        }
        return result;
    }
    
    // ConversionTarget interface

    @Override
    public void putNull() {
        internalPut(null);
    }

    @Override
    public void putDate(long value) {
        internalPut(value);
    }

    @Override
    public void putDateTime(long value) {
        internalPut(value);
    }

    @Override
    public void putDecimal(BigDecimal value) {
        internalPut(value);
    }

    @Override
    public void putDouble(double value) {
        internalPut(value);
    }

    @Override
    public void putFloat(float value) {
        internalPut(value);
    }

    @Override
    public void putInt(long value) {
        internalPut(value);
    }

    @Override
    public void putLong(long value) {
        internalPut(value);
    }

    @Override
    public void putString(String value) {
        internalPut(value);
    }

    @Override
    public void putText(String value) {
        internalPut(value);
    }

    @Override
    public void putTime(long value) {
        internalPut(value);
    }

    @Override
    public void putTimestamp(long value) {
        internalPut(value);
    }

    @Override
    public void putUBigInt(BigInteger value) {
        internalPut(value);
    }

    @Override
    public void putUDouble(double value) {
        internalPut(value);
    }

    @Override
    public void putUFloat(float value) {
        internalPut(value);
    }

    @Override
    public void putUInt(long value) {
        internalPut(value);
    }

    @Override
    public void putVarBinary(ByteSource value) {
        internalPut(value);
    }

    @Override
    public void putYear(long value) {
        internalPut(value);
    }

    @Override
    public AkType getConversionType() {
        return akType;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("Converted(%s %s)",
                akType,
                putPending ? "<put pending>" : result
        );
    }

    // for use in this class
    
    private void internalPut(Object value) {
        if (!putPending) {
            throw new IllegalStateException("no put pending: " + toString());
        }
        result = value;
        putPending = false;
    }
    
    // object state
    
    private Object result;
    private AkType akType = AkType.UNSUPPORTED;
    private boolean putPending = true;
}
