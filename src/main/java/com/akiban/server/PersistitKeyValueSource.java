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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;
import com.persistit.Key;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PersistitKeyValueSource implements ValueSource {

    // PersistitKeyValueSource interface

    public void attach(Key key, IndexColumn indexColumn) {
        attach(key, indexColumn.getPosition(), indexColumn.getColumn().getType().akType());
    }

    public void attach(Key key, int depth, AkType type) {
        if (type == AkType.INTERVAL_MILLIS)
            throw new UnsupportedOperationException();
        this.key = key;
        this.key.indexTo(depth);
        this.akType = type;
        clear();
    }

    // ValueSource interface

    @Override
    public boolean isNull() {
        return decode().isNull();
    }

    @Override
    public BigDecimal getDecimal() {
        return decode().getDecimal();
    }

    @Override
    public BigInteger getUBigInt() {
        return decode().getUBigInt();
    }

    @Override
    public ByteSource getVarBinary() {
        return decode().getVarBinary();
    }

    @Override
    public double getDouble() {
        return decode().getDouble();
    }

    @Override
    public double getUDouble() {
        return decode().getUDouble();
    }

    @Override
    public float getFloat() {
        return decode().getFloat();
    }

    @Override
    public float getUFloat() {
        return decode().getUFloat();
    }

    @Override
    public long getDate() {
        return decode().getDate();
    }

    @Override
    public long getDateTime() {
        return decode().getDateTime();
    }

    @Override
    public long getInt() {
        return decode().getInt();
    }

    @Override
    public long getLong() {
        return decode().getLong();
    }

    @Override
    public long getTime() {
        return decode().getTime();
    }

    @Override
    public long getTimestamp() {
        return decode().getTimestamp();
    }
    
    @Override
    public long getInterval_Millis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getUInt() {
        return decode().getUInt();
    }

    @Override
    public long getYear() {
        return decode().getYear();
    }

    @Override
    public String getString() {
        return decode().getString();
    }

    @Override
    public String getText() {
        return decode().getText();
    }

    @Override
    public boolean getBool() {
        return decode().getBool();
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        // Can we optimize this at all?
        AkType type = getConversionType();
        quote.quote(appender, type);
        quote.append(appender, getString());
        quote.quote(appender, type);
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
    
    private ValueSource decode() {
        if (needsDecoding) {
            int oldIndex = key.getIndex();
            if (key.isNull()) {
                valueHolder.putNull();
            }
            else
            {
                switch (akType.underlyingType()) {
                    case BOOLEAN_AKTYPE:valueHolder.putBool(key.decodeBoolean());       break;
                    case LONG_AKTYPE:   valueHolder.putRaw(akType, key.decodeLong());   break;
                    case FLOAT_AKTYPE:  valueHolder.putRaw(akType, key.decodeFloat());  break;
                    case DOUBLE_AKTYPE: valueHolder.putRaw(akType, key.decodeDouble()); break;
                    case OBJECT_AKTYPE: valueHolder.putRaw(akType, key.decode());       break;
                    default: throw new UnsupportedOperationException(akType.name());
                }
            }
            key.indexTo(oldIndex);
            needsDecoding = false;
        }
        return valueHolder;
    }
    
    private void clear() {
        needsDecoding = true;
    }

    // object state

    private Key key;
    private AkType akType = AkType.UNSUPPORTED;
    private ValueHolder valueHolder = new ValueHolder();
    private boolean needsDecoding = true;
}
