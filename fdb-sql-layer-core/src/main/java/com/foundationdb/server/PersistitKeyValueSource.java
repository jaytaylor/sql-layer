/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server;

import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;
import com.foundationdb.server.types.value.Value;

import java.util.EnumMap;

import static java.lang.Math.min;

public class PersistitKeyValueSource implements ValueSource {

    // object state
    private Key key;
    private int depth;
    private TInstance type;
    private Value output;
    private boolean needsDecoding = true;
    
    public PersistitKeyValueSource(TInstance type) {
        this.type = type;
        this.output = new Value(type);
    }
    
    public void attach(Key key, IndexColumn indexColumn) {
        attach(key, indexColumn.getPosition(), indexColumn.getColumn().getType());
    }

    public void attach(Key key, int depth, TInstance type) {
        this.key = key;
        this.depth = depth;
        this.type = type;
        clear();
    }
    
    public void attach(Key key) {
        this.key = key;
        clear();
    }

    @Override
    public TInstance getType() {
        return type;
    }

    @Override
    public boolean hasAnyValue() {
        return decode().hasAnyValue();
    }

    @Override
    public boolean hasRawValue() {
        return decode().hasRawValue();
    }

    @Override
    public boolean hasCacheValue() {
        return decode().hasCacheValue();
    }

    @Override
    public boolean canGetRawValue() {
        return decode().canGetRawValue();
    }

    @Override
    public boolean isNull() {
        /*
         * No need to decode the value to detect null
         */
        if (needsDecoding) {
            key.indexTo(depth);
            return key.isNull();
        }
        return decode().isNull();
    }

    @Override
    public boolean getBoolean() {
        return decode().getBoolean();
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return decode().getBoolean(defaultValue);
    }

    @Override
    public byte getInt8() {
        return decode().getInt8();
    }

    @Override
    public short getInt16() {
        return decode().getInt16();
    }

    @Override
    public char getUInt16() {
        return decode().getUInt16();
    }

    @Override
    public int getInt32() {
        return decode().getInt32();
    }

    @Override
    public long getInt64() {
        return decode().getInt64();
    }

    @Override
    public float getFloat() {
        return decode().getFloat();
    }

    @Override
    public double getDouble() {
        return decode().getDouble();
    }

    @Override
    public byte[] getBytes() {
        return decode().getBytes();
    }

    @Override
    public String getString() {
        return decode().getString();
    }

    @Override
    public Object getObject() {
        return decode().getObject();
    }
    
    public int compare(PersistitKeyValueSource that)
    {
        that.key.indexTo(that.depth);
        int thatPosition = that.key.getIndex();
        that.key.indexTo(that.depth + 1);
        int thatEnd = that.key.getIndex();
        return compareOneKeySegment(that.key.getEncodedBytes(), thatPosition, thatEnd);
    }

    public int compare(byte[] bytes)
    {
        Key thatKey = new Key(key);
        thatKey.clear();
        thatKey.append(bytes);
        thatKey.indexTo(0);
        int thatPosition = thatKey.getIndex();
        thatKey.indexTo(1);
        int thatEnd = thatKey.getIndex();
        return compareOneKeySegment(thatKey.getEncodedBytes(), thatPosition, thatEnd);
    }

    // for use by this class
    private ValueSource decode() {
        if (needsDecoding) {
            key.indexTo(depth);
            if (key.getEncodedSize() == 0 || key.isNull()) {
                output.putNull();
            }
            else
            {
                UnderlyingType underlyingType = TInstance.underlyingType(getType());
                Class<?> expected = underlyingExpectedClasses.get(underlyingType);
                if (key.decodeType() == expected) {
                    switch (underlyingType) {
                        case BOOL:      output.putBool(key.decodeBoolean());        break;
                        case INT_8:     output.putInt8((byte)key.decodeLong());     break;
                        case INT_16:    output.putInt16((short)key.decodeLong());   break;
                        case UINT_16:   output.putUInt16((char)key.decodeLong());   break;
                        case INT_32:    output.putInt32((int)key.decodeLong());     break;
                        case INT_64:    output.putInt64(key.decodeLong());          break;
                        case FLOAT:     output.putFloat(key.decodeFloat());         break;
                        case DOUBLE:    output.putDouble(key.decodeDouble());       break;
                        case BYTES:     output.putBytes(key.decodeByteArray());     break;
                        case STRING:    output.putString(key.decodeString(), null); break;
                        default: throw new UnsupportedOperationException(type + " with " + underlyingType);
                    }
                }
                else {
                    output.putObject(key.decode());
                }
                // the following asumes that the TClass' readCollating expects the same UnderlyingType for in and out
                type.readCollating(output, output);
            }
            needsDecoding = false;
        }
        return output;
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

    private static final EnumMap<UnderlyingType, Class<?>> underlyingExpectedClasses = createPUnderlyingExpectedClasses();

    private static EnumMap<UnderlyingType, Class<?>> createPUnderlyingExpectedClasses() {
        EnumMap<UnderlyingType, Class<?>> result = new EnumMap<>(UnderlyingType.class);
        for (UnderlyingType underlyingType : UnderlyingType.values()) {
            final Class<?> expected;
            switch (underlyingType) {
            case BOOL:
                expected = Boolean.class;
                break;
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
            case INT_64:
                expected = Long.class;
                break;
            case FLOAT:
                expected = Float.class;
                break;
            case DOUBLE:
                expected = Double.class;
                break;
            case BYTES:
                expected = byte[].class;
                break;
            case STRING:
                expected = String.class;
                break;
            default:
                throw new AssertionError("unrecognized UnderlyingType: " + underlyingType);
            }
            result.put(underlyingType, expected);
        }
        return result;
    }
}
