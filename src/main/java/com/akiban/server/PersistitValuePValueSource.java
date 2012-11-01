/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
            persistitValue.skip(); // need to advance to next object in the stream
            cacheObject = NULL;
        }
        else {
            Class<?> valueClass = persistitValue.getType();
            // TODO This is a workaround for bug 1073357. When that's fixed, we should remove this block
            // and the rawObject field, and we should uncomment the code at the end of this method
            Object decoded;
            if (valueClass == Object.class) {
                decoded = persistitValue.get();
                valueClass = decoded.getClass();
            }
            else {
                decoded = null;
            }
            // end workaround block

            PUnderlying rawUnderlying = classesToUnderlying.get(valueClass);
            if (rawUnderlying != null) {
                pValue.underlying(rawUnderlying);
                cacheObject = null;
                rawObject = decoded;
            }
            // part of the same workaround
            else {
                cacheObject = decoded;
                rawObject = null;
            }
// TODO uncomment this when bug 1073357 is fixed.
//            else
//                cacheObject = READY_FOR_CACHE;
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
        return (cacheObject != null) && (cacheObject != NULL);
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
            pValue.putBytes(rawObject == null ? persistitValue.getByteArray() : (byte[])rawObject);
        return pValue.getBytes();
    }

    @Override
    public String getString() {
        if (needsDecoding(PUnderlying.STRING))
            pValue.putString(rawObject == null ? persistitValue.getString() : (String) rawObject, null);
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
    private Object rawObject;
    
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
