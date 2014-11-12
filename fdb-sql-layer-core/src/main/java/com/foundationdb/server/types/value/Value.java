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

package com.foundationdb.server.types.value;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;

public final class Value implements ValueSource, ValueTarget {
    // Value interface
    
    public void underlying(TInstance underlying) {
        this.type = underlying;
        this.state = State.UNSET;
    }

    public void unset() {
        this.state = State.UNSET;
    }
    
    // ValueTarget interface

    @Override
    public boolean supportsCachedObjects() {
        return true;
    }

    @Override
    public final void putNull() {
        // TODO should this check that the type is nullable?
        setRawValues(State.NULL, -1, null, null);
    }

    @Override
    public void putBool(boolean value) {
        setIVal(UnderlyingType.BOOL, value ? BOOL_TRUE : BOOL_FALSE);
    }

    @Override
    public final void putInt8(byte value) {
        setIVal(UnderlyingType.INT_8, value);
    }

    @Override
    public final void putInt16(short value) {
        setIVal(UnderlyingType.INT_16, value);
    }

    @Override
    public final void putUInt16(char value) {
        setIVal(UnderlyingType.UINT_16, value);
    }

    @Override
    public final void putInt32(int value) {
        setIVal(UnderlyingType.INT_32, value);
    }

    @Override
    public final void putInt64(long value) {
        setIVal(UnderlyingType.INT_64, value);
    }

    @Override
    public final void putFloat(float value) {
        setIVal(UnderlyingType.FLOAT, Float.floatToIntBits(value));
    }

    @Override
    public final void putDouble(double value) {
        setIVal(UnderlyingType.DOUBLE, Double.doubleToLongBits(value));
    }

    @Override
    public final void putBytes(byte[] value) {
        checkUnderlying(UnderlyingType.BYTES);
        setRawValues(State.VAL_ONLY, -1, value, null);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        checkUnderlying(UnderlyingType.STRING);
        setRawValues(State.VAL_ONLY, -1, value, null);
    }

    @Override
    public final void putObject(Object object) {
        if (object == null)
            putNull();
        else
            setRawValues(State.CACHE_ONLY, -1, null, object);
    }

    // ValueSource interface

    @Override
    public final boolean isNull() {
        if (state == State.UNSET)
            throw new IllegalStateException("state not set");
        return state == State.NULL;
    }

    @Override
    public boolean getBoolean() {
        return getIVal(UnderlyingType.BOOL) == BOOL_TRUE;
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return isNull() ? defaultValue : getBoolean();
    }

    @Override
    public final byte getInt8() {
        return (byte) getIVal(UnderlyingType.INT_8);
    }

    @Override
    public final short getInt16() {
        return (short) getIVal(UnderlyingType.INT_16);
    }

    @Override
    public final char getUInt16() {
        return (char) getIVal(UnderlyingType.UINT_16);
    }

    @Override
    public final int getInt32() {
        return (int) getIVal(UnderlyingType.INT_32);
    }

    @Override
    public final long getInt64() {
        return getIVal(UnderlyingType.INT_64);
    }

    @Override
    public final float getFloat() {
        int i = (int) getIVal(UnderlyingType.FLOAT);
        return Float.intBitsToFloat(i);
    }

    @Override
    public final double getDouble() {
        long l = getIVal(UnderlyingType.DOUBLE);
        return Double.longBitsToDouble(l);
    }

    @Override
    public final byte[] getBytes() {
        checkUnderlying(UnderlyingType.BYTES);
        checkRawState();
        return (byte[]) rawObject;
    }

    @Override
    public String getString() {
        checkUnderlying(UnderlyingType.STRING);
        checkRawState();
        return (String) rawObject;
    }

    @Override
    public final Object getObject() {
        ensureCached();
        assert state != State.UNSET : State.UNSET;
        return oCache;
    }

    @Override
    public boolean hasAnyValue() {
        return state != State.UNSET;
    }

    @Override
    public boolean hasRawValue() {
        switch (state) {
        case UNSET:
        case CACHE_ONLY:
            return false;
        case NULL:
        case VAL_ONLY:
        case VAL_AND_CACHE:
            return true;
        default:
            throw new AssertionError(state);
        }
    }

    @Override
    public boolean hasCacheValue() {
        switch (state) {
        case UNSET:
        case VAL_ONLY:
            return false;
        case NULL:
        case CACHE_ONLY:
        case VAL_AND_CACHE:
            return true;
        default:
            throw new AssertionError(state);
        }
    }

    @Override
    public boolean canGetRawValue() {
        if (hasRawValue())
            return true;
        ValueCacher cacher = type.typeClass().cacher();
        return cacher != null && cacher.canConvertToValue(oCache);
    }

    // ValueSource + ValueTarget

    @Override
    public TInstance getType() {
        return type;
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Value(").append(type).append(" = ");
        switch (state) {
        case UNSET:
            sb.append("<empty>");
            break;
        case NULL:
            sb.append("NULL");
            break;
        case VAL_AND_CACHE:
            sb.append("cached <").append(oCache).append(">: ");
        case VAL_ONLY:
            switch (TInstance.underlyingType(type)) {
            case INT_8:
            case INT_16:
            case INT_32:
            case INT_64:
            case UINT_16:
                sb.append(iVal);
                break;
            case FLOAT:
                sb.append(getFloat());
                break;
            case DOUBLE:
                sb.append(getDouble());
                break;
            case BOOL:
                sb.append(getBoolean());
                break;
            case STRING:
                sb.append('"').append(getString()).append('"');
                break;
            case BYTES:
                sb.append("0x ");
                byte[] bVal = (byte[]) rawObject;
                for (int i = 0, max= bVal.length; i < max; ++i) {
                    byte b = bVal[i];
                    int bInt = ((int)b) & 0xFF;
                    if (i > 0 && (i % 2 == 0))
                        sb.append(' ');
                    sb.append(Integer.toHexString(bInt).toUpperCase());
                }
                break;
            }
            break;
        case CACHE_ONLY:
            sb.append("(cached) ").append(oCache);
            break;
        }
        sb.append(')');
        return sb.toString();
    }

    private long getIVal(UnderlyingType expectedType) {
        checkUnderlying(expectedType);
        checkRawState();
        return iVal;
    }

    private void checkUnderlying(UnderlyingType expected) {
        UnderlyingType underlyingType = TInstance.underlyingType(type);
        if (underlyingType != expected) {
            String underlyingToString = (underlyingType == null) ? "unspecified" : underlyingType.name();
            throw new IllegalStateException("required underlying " + expected + " but was " + underlyingToString);
        }
    }

    private void setIVal(UnderlyingType expectedType, long value) {
        checkUnderlying(expectedType);
        setRawValues(State.VAL_ONLY, value, null, null);
    }

    private void setRawValues(State state, long iVal, Object rawObject, Object oCache) {
        this.state = state;
        this.iVal = iVal;
        this.rawObject = rawObject;
        this.oCache = oCache;
    }

    public Value() {
        this(null);
    }

    public Value(TInstance type) {
        underlying(type);
    }

    public Value(TInstance type, byte[] val) {
        this(type);
        putBytes(val);
    }

    public Value(TInstance type, long val) {
        this(type);
        putInt64(val);
    }

    public Value(TInstance type, float val)
    {
        this(type);
        putFloat(val);
    }

    public Value(TInstance type, double val)
    {
        this(type);
        putDouble(val);
    }

    public Value(TInstance type, int val) {
        this(type);
        putInt32(val);
    }

    public Value(TInstance type, short val) {
        this(type);
        putInt16(val);
    }

    public Value(TInstance type, byte val) {
        this(type);
        putInt8(val);
    }

    public Value(TInstance type, String val) {
        this(type);
        putString(val, null);
    }

    public Value(TInstance type, boolean val) {
        this(type);
        putBool(val);
    }

    private void checkRawState() {
        switch (state) {
        case UNSET:
        case CACHE_ONLY:
            Object oCacheSave = oCache;
            type.typeClass().cacher().cacheToValue(oCacheSave, type, this);
            assert state == State.VAL_ONLY : state;
            oCache = oCacheSave;
            state = State.VAL_AND_CACHE;
            break;
        case NULL:
            throw new NullValueException();
        case VAL_ONLY:
        case VAL_AND_CACHE:
            break;
        default:
            throw new AssertionError(state);
        }
    }

    private void ensureCached() {
        switch (state) {
        case UNSET:
            throw new IllegalStateException("no value set");
        case VAL_ONLY:
            oCache = type.typeClass().cacher().valueToCache(this, type);
            state = State.VAL_AND_CACHE;
            break;
        case NULL:
        case CACHE_ONLY:
        case VAL_AND_CACHE:
            break;
        default:
            throw new AssertionError(state);
        }
    }

    private TInstance type;
    private State state;
    private long iVal;
    private Object rawObject;
    private Object oCache;

    private static final long BOOL_TRUE = 1L;
    private static final long BOOL_FALSE = 0L;

    private enum State {
        UNSET, NULL, VAL_ONLY, CACHE_ONLY, VAL_AND_CACHE
    }
}
