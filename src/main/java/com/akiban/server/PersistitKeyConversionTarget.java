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

import com.akiban.server.types.ConversionTarget;
import com.akiban.util.ByteSource;
import com.persistit.Key;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PersistitKeyConversionTarget implements ConversionTarget {

    // KeyConversionTarget interface

    public void attach(Key key) {
        this.key = key;
    }

    // ConversionTarget interface

    @Override
    public void putNull() {
        key.append(null);
    }

    @Override
    public void putDate(long value) {
        key.append(value);
    }

    @Override
    public void putDateTime(long value) {
        key.append(value);
    }

    @Override
    public void putDecimal(BigDecimal value) {
        key.append(value);
    }

    @Override
    public void putDouble(double value) {
        key.append(value);
    }

    @Override
    public void putFloat(float value) {
        key.append(value);
    }

    @Override
    public void putInt(long value) {
        key.append(value);
    }

    @Override
    public void putLong(long value) {
        key.append(value);
    }

    @Override
    public void putString(String value) {
        key.append(value);
    }

    @Override
    public void putText(String value) {
        key.append(value);
    }

    @Override
    public void putTime(long value) {
        key.append(value);
    }

    @Override
    public void putTimestamp(long value) {
        key.append(value);
    }

    @Override
    public void putUBigInt(BigInteger value) {
        key.append(value);
    }

    @Override
    public void putUDouble(double value) {
        key.append(value);
    }

    @Override
    public void putUFloat(float value) {
        key.append(value);
    }

    @Override
    public void putUInt(long value) {
        key.append(value);
    }

    @Override
    public void putVarBinary(ByteSource value) {
        key.appendByteArray(value.byteArray(), value.byteArrayOffset(), value.byteArrayLength());
    }

    @Override
    public void putYear(long value) {
        key.append(value);
    }

    // object interface

    @Override
    public String toString() {
        return key.toString();
    }

    // object state

    private Key key;
}
