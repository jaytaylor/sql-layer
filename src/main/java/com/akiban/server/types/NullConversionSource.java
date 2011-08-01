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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public final class NullConversionSource implements ConversionSource {

    public static ConversionSource only() {
        return INSTANCE;
    }
    
    // ConversionSource interface

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public BigDecimal getDecimal() {
        throw new SourceIsNullException();
    }

    @Override
    public BigInteger getUBigInt() {
        throw new SourceIsNullException();
    }

    @Override
    public ByteBuffer getVarBinary() {
        throw new SourceIsNullException();
    }

    @Override
    public double getDouble() {
        throw new SourceIsNullException();
    }

    @Override
    public double getUDouble() {
        throw new SourceIsNullException();
    }

    @Override
    public float getFloat() {
        throw new SourceIsNullException();
    }

    @Override
    public float getUFloat() {
        throw new SourceIsNullException();
    }

    @Override
    public long getDate() {
        throw new SourceIsNullException();
    }

    @Override
    public long getDateTime() {
        throw new SourceIsNullException();
    }

    @Override
    public long getInt() {
        throw new SourceIsNullException();
    }

    @Override
    public long getLong() {
        throw new SourceIsNullException();
    }

    @Override
    public long getTime() {
        throw new SourceIsNullException();
    }

    @Override
    public long getTimestamp() {
        throw new SourceIsNullException();
    }

    @Override
    public long getUInt() {
        throw new SourceIsNullException();
    }

    @Override
    public long getYear() {
        throw new SourceIsNullException();
    }

    @Override
    public String getString() {
        throw new SourceIsNullException();
    }

    @Override
    public String getText() {
        throw new SourceIsNullException();
    }
// hidden ctor

    private NullConversionSource() {}
    
    // class state

    private static final NullConversionSource INSTANCE = new NullConversionSource();
}
