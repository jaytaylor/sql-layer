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

package com.akiban.server.types3.pvalue;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types3.TInstance;

public final class PValue implements PValueSource, PValueTarget {
    // PValue interface
    
    public void underlying(PUnderlying underlying) {
        this.underlying = underlying;
        this.state = State.UNSET;
    }

    public void unset() {
        this.state = State.UNSET;
    }

    public void ensureRaw(PValueCacher cacher, TInstance tInstance) {
        switch (state) {
        case UNSET:
            throw new IllegalStateException("no value set");
        case CACHE_ONLY:
            Object oCacheSave = this.oCache;
            cacher.cacheToValue(oCacheSave, tInstance, this);
            assert state == State.VAL_ONLY;
            this.oCache = oCacheSave;
            state = State.VAL_AND_CACHE;
            break;
        case NULL:
        case VAL_ONLY:
        case VAL_AND_CACHE:
            break;
        default:
            throw new AssertionError(state);
        }
    }
    
    // PValueTarget interface

    @Override
    public boolean supportsCachedObjects() {
        return true;
    }

    @Override
    public final void putNull() {
        setRawValues(State.NULL, -1, null, null);
    }

    @Override
    public void putBool(boolean value) {
        setIVal(PUnderlying.BOOL, value ? BOOL_TRUE : BOOL_FALSE);
    }

    @Override
    public final void putInt8(byte value) {
        setIVal(PUnderlying.INT_8, value);
    }

    @Override
    public final void putInt16(short value) {
        setIVal(PUnderlying.INT_16, value);
    }

    @Override
    public final void putUInt16(char value) {
        setIVal(PUnderlying.UINT_16, value);
    }

    @Override
    public final void putInt32(int value) {
        setIVal(PUnderlying.INT_32, value);
    }

    @Override
    public final void putInt64(long value) {
        setIVal(PUnderlying.INT_64, value);
    }

    @Override
    public final void putFloat(float value) {
        setIVal(PUnderlying.FLOAT, Float.floatToIntBits(value));
    }

    @Override
    public final void putDouble(double value) {
        setIVal(PUnderlying.DOUBLE, Double.doubleToLongBits(value));
    }

    @Override
    public final void putBytes(byte[] value) {
        checkUnderlying(PUnderlying.BYTES);
        setRawValues(State.VAL_ONLY, -1, value, null);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        checkUnderlying(PUnderlying.STRING);
        setRawValues(State.VAL_ONLY, -1, null, value);
    }

    @Override
    public final void putObject(Object object) {
        if (object == null)
            putNull();
        else
            setRawValues(State.CACHE_ONLY, -1, null, object);
    }

    // PValueSource interface

    @Override
    public final boolean isNull() {
        if (state == State.UNSET)
            throw new IllegalStateException("state not set");
        return state == State.NULL;
    }

    @Override
    public boolean getBoolean() {
        return getIVal(PUnderlying.BOOL) == BOOL_TRUE;
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return isNull() ? defaultValue : getBoolean();
    }

    @Override
    public final byte getInt8() {
        return (byte) getIVal(PUnderlying.INT_8);
    }

    @Override
    public final short getInt16() {
        return (short) getIVal(PUnderlying.INT_16);
    }

    @Override
    public final char getUInt16() {
        return (char) getIVal(PUnderlying.UINT_16);
    }

    @Override
    public final int getInt32() {
        return (int) getIVal(PUnderlying.INT_32);
    }

    @Override
    public final long getInt64() {
        return getIVal(PUnderlying.INT_64);
    }

    @Override
    public final float getFloat() {
        int i = (int) getIVal(PUnderlying.FLOAT);
        return Float.intBitsToFloat(i);
    }

    @Override
    public final double getDouble() {
        long l = getIVal(PUnderlying.DOUBLE);
        return Double.longBitsToDouble(l);
    }

    @Override
    public final byte[] getBytes() {
        checkUnderlying(PUnderlying.BYTES);
        internalUpdateRaw();
        return bVal;
    }

    @Override
    public String getString() {
        checkUnderlying(PUnderlying.STRING);
        return (String) oCache;
    }

    @Override
    public final Object getObject() {
        switch (state) {
        case UNSET:
            throw new IllegalStateException("no value set");
        case NULL:
            return null;
        case VAL_ONLY:
            throw new IllegalArgumentException("no cached object set");
            // fall through
        case CACHE_ONLY:
        case VAL_AND_CACHE:
            return oCache;
        default:
            throw new AssertionError(state);
        }
    }

    @Override
    public boolean hasAnyValue() {
        return state != State.UNSET;
    }

    @Override
    public boolean hasRawValue() {
        return state == State.VAL_ONLY || state == State.VAL_AND_CACHE;
    }

    @Override
    public boolean hasCacheValue() {
        return state == State.CACHE_ONLY || state == State.VAL_AND_CACHE;
    }

    // PValueSource + PValueTarget

    @Override
    public PUnderlying getUnderlyingType() {
        return underlying;
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PValue(").append(underlying).append(" = ");
        switch (state) {
        case UNSET:
            sb.append("<empty>");
            break;
        case NULL:
            sb.append("NULL");
            break;
        case VAL_ONLY:
            switch (underlying) {
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
                sb.append(getString());
                break;
            case BYTES:
                sb.append("0x ");
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
        case VAL_AND_CACHE:
            sb.append("(cached) ").append(oCache);
            break;
        }
        sb.append(')');
        return sb.toString();
    }

    private long getIVal(PUnderlying expectedType) {
        checkUnderlying(expectedType);
        internalUpdateRaw();
        return iVal;
    }

    private void internalUpdateRaw() {
        switch (state) {
        case UNSET:
            throw new IllegalStateException("no value set");
        case NULL:
            throw new NullValueException();
        case CACHE_ONLY:
            // TODO
            assert false;
            // fall through
        case VAL_ONLY:
        case VAL_AND_CACHE:
            break;
        default:
            throw new AssertionError(state);
        }
    }

    private void checkUnderlying(PUnderlying expected) {
        if (underlying != expected) {
            String underlyingToString = (underlying == null) ? "unspecified" : underlying.name();
            throw new IllegalStateException("required underlying " + expected + " but was " + underlyingToString);
        }
    }

    private void setIVal(PUnderlying expectedType, long value) {
        checkUnderlying(expectedType);
        setRawValues(State.VAL_ONLY, value, null, null);
    }

    private void setRawValues(State state, long iVal, byte[] bVal, Object oCache) {
        this.state = state;
        this.iVal = iVal;
        this.bVal = bVal;
        this.oCache = oCache;
    }

    public PValue() {
        this((PUnderlying)null);
    }

    public PValue(PUnderlying underlying) {
        underlying(underlying);
    }

    public PValue(byte[] val) {
        this(PUnderlying.BYTES);
        putBytes(val);
    }

    public PValue(long val) {
        this(PUnderlying.INT_64);
        putInt64(val);
    }

    public PValue(float val)
    {
        this(PUnderlying.FLOAT);
        putFloat(val);
    }

    public PValue(double val)
    {
        this(PUnderlying.DOUBLE);
        putDouble(val);
    }

    public PValue(int val) {
        this(PUnderlying.INT_32);
        putInt32(val);
    }

    public PValue(short val) {
        this(PUnderlying.INT_16);
        putInt16(val);
    }

    public PValue(String val) {
        this(PUnderlying.STRING);
        putString(val, null);
    }

    public PValue(boolean val) {
        this(PUnderlying.BOOL);
        putBool(val);
    }

    private PUnderlying underlying;
    private State state;
    private long iVal;
    private byte[] bVal;
    private Object oCache;

    private static final long BOOL_TRUE = 1L;
    private static final long BOOL_FALSE = 0L;

    private enum State {
        UNSET, NULL, VAL_ONLY, CACHE_ONLY, VAL_AND_CACHE
    }
}
