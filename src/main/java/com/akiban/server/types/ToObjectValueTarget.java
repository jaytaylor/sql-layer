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

import com.akiban.server.types.conversion.Converters;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class ToObjectValueTarget implements ValueTarget {
    
    // ToObjectValueTarget interface

    /**
     * Convenience method for extracting an Object from a conversion source.
     * @param source the incoming source
     * @return the converted Object
     */
    public Object convertFromSource(ValueSource source) {
        expectType(source.getConversionType());
        return Converters.convert(source, this).lastConvertedValue();
    }

    public ToObjectValueTarget expectType(AkType type) {
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
    
    // ValueTarget interface

    @Override
    public void putNull() {
        internalPut(null, AkType.NULL);
    }

    @Override
    public void putDate(long value) {
        internalPut(value, AkType.DATE);
    }

    @Override
    public void putDateTime(long value) {
        internalPut(value, AkType.DATETIME);
    }

    @Override
    public void putDecimal(BigDecimal value) {
        internalPut(value, AkType.DECIMAL);
    }

    @Override
    public void putDouble(double value) {
        internalPut(value, AkType.DOUBLE);
    }

    @Override
    public void putFloat(float value) {
        internalPut(value, AkType.FLOAT);
    }

    @Override
    public void putInt(long value) {
        internalPut(value, AkType.INT);
    }

    @Override
    public void putLong(long value) {
        internalPut(value, AkType.LONG);
    }

    @Override
    public void putString(String value) {
        internalPut(value, AkType.VARCHAR);
    }

    @Override
    public void putText(String value) {
        internalPut(value, AkType.TEXT);
    }

    @Override
    public void putTime(long value) {
        internalPut(value, AkType.TIME);
    }

    @Override
    public void putTimestamp(long value) {
        internalPut(value, AkType.TIMESTAMP);
    }
    
    @Override
    public void putInterval_Millis(long value){
        internalPut(value, AkType.INTERVAL_MILLIS);
    }

    @Override
    public void putInterval_Month(long value) {
        internalPut(value, AkType.INTERVAL_MONTH);
    }
    
    @Override
    public void putUBigInt(BigInteger value) {
        internalPut(value, AkType.U_BIGINT);
    }

    @Override
    public void putUDouble(double value) {
        internalPut(value, AkType.U_DOUBLE);
    }

    @Override
    public void putUFloat(float value) {
        internalPut(value, AkType.U_FLOAT);
    }

    @Override
    public void putUInt(long value) {
        internalPut(value, AkType.U_INT);
    }

    @Override
    public void putVarBinary(ByteSource value) {
        internalPut(value, AkType.VARBINARY);
    }

    @Override
    public void putYear(long value) {
        internalPut(value, AkType.YEAR);
    }

    @Override
    public void putBool(boolean value) {
        internalPut(value, AkType.BOOL);
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
    
    private void internalPut(Object value, AkType type) {
        ValueSourceHelper.checkType(akType, type);
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
