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

import com.akiban.ais.model.IndexColumn;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.SourceIsNullException;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import com.persistit.Key;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PersistitKeyConversionSource implements ConversionSource {

    // PersistitKeyConversionSource interface

    public void attach(Key key, IndexColumn indexColumn) {
        this.key = key;
        this.key.indexTo(indexColumn.getPosition());
        clear();
        this.akType = indexColumn.getColumn().getType().akType();
    }

    // ConversionSource interface

    @Override
    public boolean isNull() {
        return decode() == null;
    }

    @Override
    public BigDecimal getDecimal() {
        return as(BigDecimal.class);
    }

    @Override
    public BigInteger getUBigInt() {
        return as(BigInteger.class);
    }

    @Override
    public ByteSource getVarBinary() {
        byte[] bytes = as(byte[].class);
        return bytes == null ? null : byteSource.wrap(bytes);
    }

    @Override
    public double getDouble() {
        return as(Double.class);
    }

    @Override
    public double getUDouble() {
        return as(Double.class);
    }

    @Override
    public float getFloat() {
        return as(Float.class);
    }

    @Override
    public float getUFloat() {
        return as(Float.class);
    }

    @Override
    public long getDate() {
        return as(Long.class);
    }

    @Override
    public long getDateTime() {
        return as(Long.class);
    }

    @Override
    public long getInt() {
        return as(Long.class);
    }

    @Override
    public long getLong() {
        return as(Long.class);
    }

    @Override
    public long getTime() {
        return as(Long.class);
    }

    @Override
    public long getTimestamp() {
        return as(Long.class);
    }

    @Override
    public long getUInt() {
        return as(Long.class);
    }

    @Override
    public long getYear() {
        return as(Long.class);
    }

    @Override
    public String getString() {
        return as(String.class);
    }

    @Override
    public String getText() {
        return as(String.class);
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        // Can we optimize this at all?
        quote.append(appender, getString());
    }

    @Override
    public AkType getConversionType() {
        return akType;
    }
// object interface

    @Override
    public String toString() {
        return key.toString() + " bound to depth " + key.getDepth();
    }

    // for use by this class
    
    private Object decode() {
        if (needsDecoding) {
            int oldIndex = key.getIndex();
            decoded = key.decode();
            key.indexTo(oldIndex);
            needsDecoding = false;
        }
        return decoded;
    }
    
    private void clear() {
        needsDecoding = true;
    }

    private <T> T as(Class<T> castClass) {
        Object o = decode();
        if (o == null) {
            throw new SourceIsNullException();
        }
        try {
            return castClass.cast(o);
        } catch (ClassCastException e) {
            throw new ClassCastException("casting " + o.getClass() + " to " + castClass);
        }
    }

    // object state

    private Key key;
    private AkType akType = AkType.UNSUPPORTED;
    private Object decoded;
    private boolean needsDecoding = true;
    private final WrappingByteSource byteSource = new WrappingByteSource();
}
