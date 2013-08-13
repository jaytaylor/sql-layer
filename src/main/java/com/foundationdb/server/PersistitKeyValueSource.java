/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server;

import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.operator.Cursor;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;
import com.persistit.Key;

import java.math.BigDecimal;
import java.math.BigInteger;

import static java.lang.Math.min;

public final class PersistitKeyValueSource extends ValueSource {

    private AkCollator collator = null;

    // PersistitKeyValueSource interface

    public void attach(Key key, IndexColumn indexColumn) {
        attach(key,
               indexColumn.getPosition(),
               indexColumn.getColumn().getType().akType(),
               indexColumn.getColumn().getCollator());
    }

    public void attach(Key key, int depth, AkType type) {
        attach(key, depth, type, null);
    }

    public void attach(Key key, int depth, AkType type, AkCollator collator) {
        if (type == AkType.INTERVAL_MILLIS || type == AkType.INTERVAL_MONTH)
            throw new UnsupportedOperationException();
        this.key = key;
        this.depth = depth;
        this.akType = type;
        this.collator = collator;
        clear();
    }
    
    public void attach(Key key) {
        this.key = key;
        clear();
    }

    // ValueSource interface

    @Override
    public boolean isNull() {
        /*
         * No need to decode the value to detect null
         */
        if (needsDecoding) {
            key.indexTo(depth);
            return key.isNull();
        }
        return valueHolder.isNull();
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
    public long getInterval_Month() {
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
    public Cursor getResultSet() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        // Can we optimize this at all?
        AkType type = getConversionType();
        quote.quote(appender, type);
        quote.append(appender, String.valueOf(decode()));
        quote.quote(appender, type);
    }

    @Override
    public AkType getConversionType() {
        return akType;
    }

    // PersistitKeyValueSource interface

    public Key key()
    {
        return key;
    }

    public int depth()
    {
        return depth;
    }

    public int compare(PersistitKeyValueSource that)
    {
        that.key.indexTo(that.depth);
        int thatPosition = that.key.getIndex();
        that.key.indexTo(that.depth + 1);
        int thatEnd = that.key.getIndex();
        return compareOneKeySegment(that.key.getEncodedBytes(), thatPosition, thatEnd);
    }

    public int compare(AkCollator collator, String string)
    {
        assert collator != null;
        Key thatKey = new Key(key);
        thatKey.clear();
        collator.append(thatKey, string);
        thatKey.indexTo(0);
        int thatPosition = thatKey.getIndex();
        thatKey.indexTo(1);
        int thatEnd = thatKey.getIndex();
        return compareOneKeySegment(thatKey.getEncodedBytes(), thatPosition, thatEnd);
    }

    // object interface

    @Override
    public String toString() {
        return key.toString() + " bound to depth " + key.getDepth();
    }

    // for use by this class
    
    private ValueSource decode() {
        if (needsDecoding) {
            key.indexTo(depth);
            if (key.isNull()) {
                valueHolder.putNull();
            }
            else
            {
                if (collator != null) {
                    valueHolder.putRaw(akType, collator.decode(key));
                } else {
                    switch (akType.underlyingType()) {
                        case BOOLEAN_AKTYPE:valueHolder.putBool(key.decodeBoolean());       break;
                        case LONG_AKTYPE:   valueHolder.putRaw(akType, key.decodeLong());   break;
                        case FLOAT_AKTYPE:  valueHolder.putRaw(akType, key.decodeFloat());  break;
                        case DOUBLE_AKTYPE: valueHolder.putRaw(akType, key.decodeDouble()); break;
                        case OBJECT_AKTYPE: valueHolder.putRaw(akType, key.decode());       break;
                        default: throw new UnsupportedOperationException(akType.name());
                    }
                }
            }
            needsDecoding = false;
        }
        return valueHolder;
    }

    private int compareOneKeySegment(byte[] thatBytes, int thatPosition, int thatEnd)
    {
        this.key.indexTo(this.depth);
        int thisPosition = this.key.getIndex();
        this.key.indexTo(this.depth + 1);
        int thisEnd = this.key.getIndex();
        byte[] thisBytes = this.key.getEncodedBytes();
        // Compare until end or mismatch
        int thisN = thisEnd - thisPosition;
        int thatN = thatEnd - thatPosition;
        int n = min(thisN, thatN);
        int end = thisPosition + n;
        while (thisPosition < end) {
            int c = thisBytes[thisPosition++] - thatBytes[thatPosition++];
            if (c != 0) {
                return c;
            }
        }
        return thisN - thatN;
    }

    private void clear() {
        needsDecoding = true;
    }

    // Object state

    private Key key;
    private int depth;
    private AkType akType = AkType.UNSUPPORTED;
    private ValueHolder valueHolder = new ValueHolder();
    private boolean needsDecoding = true;
}
