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
    
    public Object lastConvertedValue() {
        return result;
    }
    
    // ConversionTarget interface

    @Override
    public void putNull() {
        result = null;
    }

    @Override
    public void putDate(long value) {
        result = value;
    }

    @Override
    public void putDateTime(long value) {
        result = value;
    }

    @Override
    public void putDecimal(BigDecimal value) {
        result = value;
    }

    @Override
    public void putDouble(double value) {
        result = value;
    }

    @Override
    public void putFloat(float value) {
        result = value;
    }

    @Override
    public void putInt(long value) {
        result = value;
    }

    @Override
    public void putLong(long value) {
        result = value;
    }

    @Override
    public void putString(String value) {
        result = value;
    }

    @Override
    public void putText(String value) {
        result = value;
    }

    @Override
    public void putTime(long value) {
        result = value;
    }

    @Override
    public void putTimestamp(long value) {
        result = value;
    }

    @Override
    public void putUBigInt(BigInteger value) {
        result = value;
    }

    @Override
    public void putUDouble(double value) {
        result = value;
    }

    @Override
    public void putUFloat(float value) {
        result = value;
    }

    @Override
    public void putUInt(long value) {
        result = value;
    }

    @Override
    public void putVarBinary(ByteSource value) {
        result = value;
    }

    @Override
    public void putYear(long value) {
        result = value;
    }

    // Object interface

    @Override
    public String toString() {
        return "Converted( " + result + " )";
    }

    // object state
    
    private Object result;
}
