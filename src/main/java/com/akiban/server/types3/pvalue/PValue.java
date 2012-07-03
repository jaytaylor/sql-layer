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

public final class PValue implements PValueSource, PValueTarget {

    // PValueTarget interface

    @Override
    public void putValueSource(PValueSource source) {
        if (source instanceof PValue) {
            PValue sourceRaw = (PValue) source;
            if (sourceRaw.underlying != this.underlying)
                throw new IllegalArgumentException("mismatched types: " + sourceRaw.underlying + " != " + underlying);
            setRawValues(sourceRaw.state, sourceRaw.iVal, sourceRaw.bVal, sourceRaw.oCache);
        }
        else {
            PValueTargets.copyFrom(source, this);
        }
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
    public void putString(String value) {
        checkUnderlying(PUnderlying.STRING);
        setRawValues(State.CACHE_ONLY, -1, null, value);
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
        return (String) getObject();
    }

    @Override
    public final Object getObject() {
        switch (state) {
        case UNSET:
            throw new IllegalStateException("no value set");
        case NULL:
            return null;
        case VAL_ONLY:
            oCache = cacher.valueToCache(this);
            state = State.VAL_AND_CACHE;
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

    // PValue interface

    public void setCacher(PValueCacher<?> cacher) {
        this.cacher = cacher;
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

    @SuppressWarnings("unchecked")
    private void internalUpdateRaw() {
        switch (state) {
        case UNSET:
            throw new IllegalStateException("no value set");
        case NULL:
            throw new NullValueException();
        case CACHE_ONLY:
            Object savedCache = oCache;
            ((PValueCacher)cacher).cacheToValue(oCache, this);
            if (state == State.NULL)
                throw new NullValueException();
            assert state == State.VAL_ONLY;
            oCache = savedCache;
            state = State.VAL_AND_CACHE;
            // fall through
        case VAL_ONLY:
        case VAL_AND_CACHE:
            break;
        default:
            throw new AssertionError(state);
        }
    }

    private void checkUnderlying(PUnderlying expected) {
        if (underlying != expected)
            throw new IllegalStateException("required underlying " + expected + " but was " + underlying);
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

    public PValue(PUnderlying underlying) {
        this(underlying, null);
    }
    
    public PValue(double val)
    {
        this(PUnderlying.DOUBLE);
        putDouble(val);
    }
    
    public PValue(PUnderlying underlying, PValueCacher<?> cacher) {
        this.underlying = underlying;
        this.state = State.UNSET;
        this.cacher = cacher;
    }

    private final PUnderlying underlying;
    private PValueCacher<?> cacher;
    private State state;
    private long iVal;
    private byte[] bVal;
    private Object oCache;

    private static final long BOOL_TRUE = 1L;
    private static final long BOOL_FALSE = 0L;

    private enum State {
        UNSET, NULL, VAL_ONLY, CACHE_ONLY, VAL_AND_CACHE
    }

//    public static void main(String[] args) {
//        PValue a = new PValue(PUnderlying.INT_32);
//        System.out.println(a);
//        a.putInt32(42);
//        System.out.println(a);
//
//        PValue b = new PValue(PUnderlying.UINT_16);
//        b.putUInt16('a');
//        System.out.println(b);
//
//        PValue c = new PValue(PUnderlying.BYTES);
//        c.putBytes(new byte[]{0x12, (byte) 0xFF, (byte) 0xCA, (byte) 0xEA, (byte) 0x21});
//        System.out.println(c);
//
//        PValue d = new PValue(PUnderlying.INT_64, new PValueCacher<BigDecimal>() {
//            @Override
//            public void cacheToValue(BigDecimal cached, PValueTarget value) {
//                String asString = cached.toPlainString();
//                asString = asString.replace(".", "");
//                long asLong = Long.parseLong(asString);
//                value.putInt64(asLong);
//            }
//
//            @Override
//            public BigDecimal valueToCache(PValueSource value) {
//                long asLong = value.getInt64();
//                StringBuilder asSb = new StringBuilder(String.format("%020d", asLong));
//                asSb.reverse();
//                asSb.insert(3, '.');
//                asSb.reverse();
//                return new BigDecimal(asSb.toString());
//            }
//        });
//        d.putInt64(123456L);
//        System.out.println(d);
//        d.getObject();
//        System.out.println(d);
//        System.out.println(d.getObject());
//        System.out.println(d.getInt64());
//        d.putObject(new BigDecimal("567.890"));
//        System.out.println(d.getInt64());
//        System.out.println(d.getObject());
//        System.out.println(d);
//        d.getInt16();
//    }
}
