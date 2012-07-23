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

import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.persistit.Value;

import java.util.HashMap;
import java.util.Map;

public final class PersistitValuePValueSource implements PValueSource {

    // PersistitValuePValueSource interface
    
    public void attach(Value value)
    {
        this.persistitValue = value;
        clear();
        value.setStreamMode(true);
    }

    public void getReady() {
        if (persistitValue.isNull()) {
            cacheObject = NULL;
        }
        else {
            Class<?> valueClass = persistitValue.getType();
            PUnderlying rawUnderlying = classesToUnderlying.get(valueClass);
            if (rawUnderlying != null) {
                pValue.underlying(rawUnderlying);
                cacheObject = null;
            }
            else
                cacheObject = READY_FOR_CACHE;
        }
    }
    
    // PValueSource interface

    @Override
    public boolean hasAnyValue() {
        return (persistitValue != null);
    }

    @Override
    public boolean hasRawValue() {
        return hasAnyValue() && (cacheObject == null);
    }

    @Override
    public boolean hasCacheValue() {
        return (cacheObject != null);
    }

    private boolean needsDecoding(PUnderlying toUnderlying) {
        assert toUnderlying == pValue.getUnderlyingType()
                : "expected underlying " + toUnderlying + " but was set up for " + pValue.getUnderlyingType();
        return ! pValue.hasRawValue();
    }
    
    @Override
    public Object getObject() {
        if (cacheObject == null)
            throw new IllegalStateException("no cache object: " + pValue);
        if (cacheObject == READY_FOR_CACHE)
            cacheObject = persistitValue.get();
        return (cacheObject == NULL) ? null : cacheObject;
    }

    @Override
    public PUnderlying getUnderlyingType() {
        assert hasRawValue() : "underlying type is only available when there is a raw value";
        return pValue.getUnderlyingType();
    }

    @Override
    public boolean isNull() {
        return cacheObject == NULL;
    }

    @Override
    public boolean getBoolean() {
        if (needsDecoding(PUnderlying.BOOL))
            pValue.putBool(persistitValue.getBoolean());
        return pValue.getBoolean();
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return isNull() ? defaultValue : getBoolean();
    }

    @Override
    public byte getInt8() {
        if (needsDecoding(PUnderlying.INT_8))
            pValue.putInt8(persistitValue.getByte());
        return pValue.getInt8();
    }

    @Override
    public short getInt16() {
        if (needsDecoding(PUnderlying.INT_16))
            pValue.putInt16(persistitValue.getShort());
        return pValue.getInt16();
    }

    @Override
    public char getUInt16() {
        if (needsDecoding(PUnderlying.UINT_16))
            pValue.putUInt16(persistitValue.getChar());
        return pValue.getUInt16();
    }

    @Override
    public int getInt32() {
        if (needsDecoding(PUnderlying.INT_32))
            pValue.putInt32(persistitValue.getInt());
        return pValue.getInt32();
    }

    @Override
    public long getInt64() {
        if (needsDecoding(PUnderlying.INT_64))
            pValue.putInt64(persistitValue.getLong());
        return pValue.getInt64();
    }

    @Override
    public float getFloat() {
        if (needsDecoding(PUnderlying.FLOAT))
            pValue.putFloat(persistitValue.getFloat());
        return pValue.getFloat();
    }

    @Override
    public double getDouble() {
        if (needsDecoding(PUnderlying.DOUBLE))
            pValue.putDouble(persistitValue.getDouble());
        return pValue.getDouble();
    }

    @Override
    public byte[] getBytes() {
        if (needsDecoding(PUnderlying.BYTES))
            pValue.putBytes(persistitValue.getByteArray());
        return pValue.getBytes();
    }

    @Override
    public String getString() {
        if (needsDecoding(PUnderlying.STRING))
            pValue.putString(persistitValue.getString(), null);
        return pValue.getString();
    }

    // private
    
    private void clear() {
        pValue.underlying(null);
    }
    
    // object state


    public PersistitValuePValueSource() {
        clear();
    }

    private Value persistitValue;
    private PValue pValue = new PValue();
    private Object cacheObject = null;
    
    private static final Object READY_FOR_CACHE = new Object();
    private static final Object NULL = new Object();
    private static final Map<Class<?>,PUnderlying> classesToUnderlying = createTranslationMap();

    private static Map<Class<?>, PUnderlying> createTranslationMap() {
        Map<Class<?>,PUnderlying> map = new HashMap<Class<?>, PUnderlying>(PUnderlying.values().length);
        map.put(boolean.class, PUnderlying.BOOL);
        map.put(byte.class, PUnderlying.INT_8);
        map.put(short.class, PUnderlying.INT_16);
        map.put(char.class, PUnderlying.UINT_16);
        map.put(int.class, PUnderlying.INT_32);
        map.put(long.class, PUnderlying.INT_64);
        map.put(float.class, PUnderlying.FLOAT);
        map.put(double.class, PUnderlying.DOUBLE);
        map.put(byte[].class, PUnderlying.BYTES);
        map.put(String.class, PUnderlying.STRING);
        return map;
    }
}
