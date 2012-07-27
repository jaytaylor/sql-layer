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

package com.akiban.server.types.util;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.Quote;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueSourceIsNullException;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.WrongValueGetException;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;

import com.akiban.util.WrappingByteSource;
import org.joda.time.DateTime;

public final class ValueHolder extends ValueSource implements ValueTarget {

    // ValueHolder interface

    public void clear() {
        type = AkType.UNSUPPORTED;
        stateType = StateType.UNDEF_VAL;
    }

    public ValueHolder copyFrom(ValueSource copySource) {
        expectType(copySource.getConversionType());
        Converters.convert(copySource, this);
        return this;
    }

    public void expectType(AkType expectedType) {
        type = expectedType;
    }

    public boolean hasSourceState() {
        return stateType != StateType.UNDEF_VAL;
    }

    // for use in this class (testing)

    void requireForPuts(AkType type) {
        allowedPut = type;
    }

    // ValueSource interface

    @Override
    public boolean isNull() {
        if (stateType == StateType.UNDEF_VAL)
            throw new IllegalStateException("ValueHolder has no state");
        return type == AkType.NULL;
    }

    @Override
    public BigDecimal getDecimal() {
        return rawObject(AkType.DECIMAL, BigDecimal.class);
    }

    @Override
    public BigInteger getUBigInt() {
        return rawObject(AkType.U_BIGINT, BigInteger.class);
    }

    @Override
    public ByteSource getVarBinary() {
        return rawObject(AkType.VARBINARY, ByteSource.class);
    }

    @Override
    public double getDouble() {
        return rawDouble(AkType.DOUBLE);
    }

    @Override
    public double getUDouble() {
        return rawDouble(AkType.U_DOUBLE);
    }

    @Override
    public float getFloat() {
        return rawFloat(AkType.FLOAT);
    }

    @Override
    public float getUFloat() {
        return rawFloat(AkType.U_FLOAT);
    }

    @Override
    public long getDate() {
        return rawLong(AkType.DATE);
    }

    @Override
    public long getDateTime() {
        return rawLong(AkType.DATETIME);
    }

    @Override
    public long getInt() {
        return rawLong(AkType.INT);
    }

    @Override
    public long getLong() {
        return rawLong(AkType.LONG);
    }

    @Override
    public long getTime() {
        return rawLong(AkType.TIME);
    }

    @Override
    public long getTimestamp() {
        return rawLong(AkType.TIMESTAMP);
    }

    @Override
    public long getInterval_Millis() {
        return rawLong(AkType.INTERVAL_MILLIS);
    }

    @Override
    public long getInterval_Month() {
        return rawLong(AkType.INTERVAL_MONTH);
    }
    
    @Override
    public long getUInt() {
        return rawLong(AkType.U_INT);
    }

    @Override
    public long getYear() {
        return rawLong(AkType.YEAR);
    }

    @Override
    public String getString() {
        return rawObject(AkType.VARCHAR, String.class);
    }

    @Override
    public String getText() {
        return rawObject(AkType.TEXT, String.class);
    }

    @Override
    public boolean getBool() {
        return rawLong(AkType.BOOL) != 0;
    }

    @Override
    public Cursor getResultSet() {
        return rawObject(AkType.RESULT_SET, Cursor.class);
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        // TODO use extractors instead
        ToObjectValueTarget target = new ToObjectValueTarget();
        target.expectType(AkType.VARCHAR);
        String string = (String) Converters.convert(this, target).lastConvertedValue();
        appender.append(string);
    }

    @Override
    public AkType getConversionType() {
        return type;
    }

    // ValueTarget interface

    @Override
    public void putNull() {
        putRawNull();
    }

    @Override
    public void putDate(long value) {
        putRaw(AkType.DATE, value);
    }

    public void putDate(DateTime value) {
        putRaw(AkType.DATE, AKTYPE_CONVERTERS.get(AkType.DATE).toLong(value));
    }

    @Override
    public void putDateTime(long value) {
        putRaw(AkType.DATETIME, value);
    }

    public void putDateTime(DateTime value) {
        putRaw(AkType.DATETIME, AKTYPE_CONVERTERS.get(AkType.DATETIME).toLong(value));
    }

    @Override
    public void putDecimal(BigDecimal value) {
        putRaw(AkType.DECIMAL, value);
    }

    @Override
    public void putDouble(double value) {
        putRaw(AkType.DOUBLE, value);
    }

    @Override
    public void putFloat(float value) {
        putRaw(AkType.FLOAT, value);
    }

    @Override
    public void putInt(long value) {
        putRaw(AkType.INT, value);
    }

    @Override
    public void putLong(long value) {
        putRaw(AkType.LONG, value);
    }

    @Override
    public void putString(String value) {
        putRaw(AkType.VARCHAR, value);
    }

    @Override
    public void putText(String value) {
        putRaw(AkType.TEXT, value);
    }

    @Override
    public void putTime(long value) {
        putRaw(AkType.TIME, value);
    }

    public void putTime(DateTime value) {
        putRaw(AkType.TIME, AKTYPE_CONVERTERS.get(AkType.TIME).toLong(value));
    }

    @Override
    public void putTimestamp(long value) {
        putRaw(AkType.TIMESTAMP, value);
    }

    public void putTimestamp(DateTime value) {
        putRaw(AkType.TIMESTAMP, AKTYPE_CONVERTERS.get(AkType.TIMESTAMP).toLong(value));
    }
    
    @Override
    public void putInterval_Millis(long value) {
        putRaw(AkType.INTERVAL_MILLIS, value);
    }

    @Override
    public void putInterval_Month(long value) {
        putRaw(AkType.INTERVAL_MONTH, value);
    }
    @Override
    public void putUBigInt(BigInteger value) {
        putRaw(AkType.U_BIGINT, value);
    }

    @Override
    public void putUDouble(double value) {
        putRaw(AkType.U_DOUBLE, value);
    }

    @Override
    public void putUFloat(float value) {
        putRaw(AkType.U_FLOAT, value);
    }

    @Override
    public void putUInt(long value) {
        putRaw(AkType.U_INT, value);
    }

    @Override
    public void putVarBinary(ByteSource value) {
        putRaw(AkType.VARBINARY, value);
    }

    @Override
    public void putYear(long value) {
        putRaw(AkType.YEAR, value);
    }

    @Override
    public void putBool(boolean value) {
        putRaw(AkType.BOOL, value ? 1L : 0L);
    }

    @Override
    public void putResultSet(Cursor value) {
        putRaw(AkType.RESULT_SET, value);
    }

    // Object interface

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueHolder that = (ValueHolder) o;
        if (type != that.type) return false;
        switch (stateType) {
        case LONG_VAL:
            if (longVal != that.longVal) return false;
            break;
        case DOUBLE_VAL:
            if (doubleVal != that.doubleVal) return false;
            break;
        case FLOAT_VAL:
            if (floatVal != that.floatVal) return false;
            break;
        case OBJECT_VAL:
            assert objectVal != null;
            assert  that.objectVal != null;
            if (!objectVal.equals(that.objectVal)) return false;
            break;
        case NULL_VAL:
        case UNDEF_VAL:
            break;
        default:
            throw new AssertionError(stateType);
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        result = type.hashCode();
        switch (stateType) {
        case LONG_VAL:
            result = (int) (longVal ^ (longVal >>> 32));
            break;
        case DOUBLE_VAL:
            long temp = doubleVal != +0.0d ? Double.doubleToLongBits(doubleVal) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            break;
        case FLOAT_VAL:
            result = 31 * result + (floatVal != +0.0f ? Float.floatToIntBits(floatVal) : 0);
            break;
        case OBJECT_VAL:
            result = 31 * result + (objectVal != null ? objectVal.hashCode() : 0);
            break;
        case NULL_VAL:
        case UNDEF_VAL:
            break;
        default:
            throw new AssertionError(stateType);
        }
        return result;
    }

    @Override
    public String toString() {
        if (isNull()) {
            return AkType.NULL.toString();
        }
        StringBuilder sb = new StringBuilder();
//        sb.append(type).append('(');
        appendAsString(AkibanAppender.of(sb), Quote.NONE);
//        sb.append(')');
        return sb.toString();
    }

    // private methods

    private void checkRawPut(AkType putType) {
        if (allowedPut != null && allowedPut != putType) {
            throw new WrongValueGetException(allowedPut, putType);
        }
    }

    public void putRaw(AkType newType, long value) {
        checkRawPut(newType);
        type = newType;
        longVal = value;
        stateType = StateType.LONG_VAL;
    }

    public void putRaw(AkType newType, double value) {
        checkRawPut(newType);
        type = newType;
        doubleVal = value;
        stateType = StateType.DOUBLE_VAL;
    }

    public void putRaw(AkType newType, float value) {
        checkRawPut(newType);
        type = newType;
        floatVal = value;
        stateType = StateType.FLOAT_VAL;
    }

    public void putRaw(AkType newType, DateTime value) {
        JodaDateToLong jdToLong = AKTYPE_CONVERTERS.get(newType);
        if (jdToLong == null)
            throw new IllegalRawPutException("DateTime() to " + newType);
        putRaw(newType, jdToLong.toLong(value));
    }

    public void putRaw(AkType newType, Object value) {
        checkRawPut(newType);
        if (value == null) {
            putRawNull();
        }
        else {
            type = newType;
            if (newType == AkType.VARBINARY && value.getClass().equals(byte[].class)) {
                value = new WrappingByteSource((byte[])value);
            }
            objectVal = value;
            stateType = StateType.OBJECT_VAL;
        }
    }

    public void putRawNull() {
        type = AkType.NULL;
        stateType = StateType.NULL_VAL;
    }

    public <T> T rawObject(AkType expectedType, Class<T> asClass) {
        checkForGet(expectedType);
        return asClass.cast(objectVal);
    }

    public double rawDouble(AkType expectedType) {
        checkForGet(expectedType);
        return doubleVal;
    }

    public float rawFloat(AkType expectedType) {
        checkForGet(expectedType);
        return floatVal;
    }

    public long rawLong(AkType expectedType) {
        checkForGet(expectedType);
        return longVal;
    }

    private void checkForGet(AkType expectedType) {
        if (isNull())
            throw new ValueSourceIsNullException();
        ValueSourceHelper.checkType(expectedType, type);
    }

    public ValueHolder() {
        clear();
    }

    public ValueHolder(AkType type, long value) {
        StateType.LONG_VAL.checkAkType(type);
        putRaw(type, value);
    }

    public ValueHolder(AkType type, double value) {
        StateType.DOUBLE_VAL.checkAkType(type);
        putRaw(type, value);
    }

    public ValueHolder(AkType type, float value) {
        StateType.FLOAT_VAL.checkAkType(type);
        putRaw(type, value);
    }

    public ValueHolder(AkType type, boolean value) {
        StateType.BOOL_VAL.checkAkType(type);
        putBool(value);
    }

    public ValueHolder(AkType type, DateTime value) {
        JodaDateToLong jdToLong = AKTYPE_CONVERTERS.get(type);
        if (jdToLong == null)
            throw new IllegalRawPutException("DateTime() to " + type);
        putRaw(type, jdToLong.toLong(value));
    }

    public ValueHolder(AkType type, Object value) {
        if (value == null) {
            putRawNull();
        }
        else {
            StateType.OBJECT_VAL.checkAkType(type, value);
            putRaw(type, value);
        }
    }

    public ValueHolder(ValueSource copySource) {
        copyFrom(copySource);
    }

    // object state

    private long longVal;
    private double doubleVal;
    private float floatVal;
    private Object objectVal;
    private AkType type = AkType.UNSUPPORTED;
    private StateType stateType = StateType.UNDEF_VAL;
    private AkType allowedPut = null;

    public static ValueHolder holdingNull() {
        ValueHolder result = new ValueHolder();
        result.putNull();
        return result;
    }

    // nested classes
    protected interface JodaDateToLong {
        long toLong (DateTime date);
    }

    private static final EnumMap<AkType, JodaDateToLong> AKTYPE_CONVERTERS = new EnumMap(AkType.class);
    static{
        AKTYPE_CONVERTERS.put(AkType.DATE, new JodaDateToLong () {
            @Override // DD + 32 * MM + 512 * YYYY
            public long toLong(DateTime date){
                int d = date.getDayOfMonth();
                int m = date.getMonthOfYear();
                int y = date.getYear();
                return d + m * 32 + y * 512;
            }
        });

        AKTYPE_CONVERTERS.put(AkType.TIME, new JodaDateToLong () {
            @Override //HH*10000 + MM*100 + SS.
            public long toLong(DateTime date){
                int h = date.getHourOfDay(); 
                int m = date.getMinuteOfHour(); 
                int s = date.getSecondOfMinute();
                return h*10000 + m*100 + s;
            }
        });

        AKTYPE_CONVERTERS.put(AkType.DATETIME, new JodaDateToLong(){
            @Override //(YY*10000 MM*100 + DD)*1000000 + (HH*10000 + MM*100 + SS)
            public long toLong(DateTime date){
                int yy = date.getYear(); 
                int mm = date.getMonthOfYear(); 
                int dd = date.getDayOfMonth(); 
                int h = date.getHourOfDay(); 
                int m = date.getMinuteOfHour(); 
                int s = date.getSecondOfMinute();               
                return (long)(yy * 10000 + mm * 100 + dd) *1000000 + h*10000 + m*100 + s;
            }
        });

        AKTYPE_CONVERTERS.put(AkType.TIMESTAMP, new JodaDateToLong() {
            @Override
            public long toLong(DateTime date){
                return date.getMillis()  / 1000;
            }
        });
    }

    private enum StateType {
        LONG_VAL (AkType.DATE, AkType.DATETIME, AkType.INT, AkType.LONG, AkType.TIME, AkType.TIMESTAMP, AkType.INTERVAL_MILLIS, AkType.INTERVAL_MONTH, AkType.U_INT, AkType.YEAR),
        DOUBLE_VAL(AkType.DOUBLE, AkType.U_DOUBLE),
        FLOAT_VAL(AkType.FLOAT, AkType.U_FLOAT),
        OBJECT_VAL(AkType.DECIMAL, AkType.VARCHAR, AkType.TEXT, AkType.U_BIGINT, AkType.VARBINARY, AkType.RESULT_SET),
        BOOL_VAL(AkType.BOOL),
        NULL_VAL(AkType.NULL),
        UNDEF_VAL()
        ;

        public void checkAkType(AkType type) {
            if (this == OBJECT_VAL || !allowed.contains(type)) {
                throw new IllegalRawPutException(this, type);
            }
        }

        public void checkAkType(AkType type, Object value) {
            if (this != OBJECT_VAL)
                throw new IllegalRawPutException(this, type);
            boolean oneCheckedOut =
                       checkObjectClass(AkType.VARCHAR, type, String.class, value)
                    || checkObjectClass(AkType.DECIMAL, type, BigDecimal.class, value)
                    || checkObjectClass(AkType.TEXT, type, String.class, value)
                    || checkObjectClass(AkType.U_BIGINT, type, BigInteger.class, value)
                    || checkObjectClass(AkType.VARBINARY, type, ByteSource.class, value)
                    || checkObjectClass(AkType.RESULT_SET, type, Cursor.class, value)
                    ;
            if (!oneCheckedOut) {
                throw new IllegalRawPutException(this, type);
            }
        }

        private boolean checkObjectClass(AkType expected, AkType actual, Class<?> expectedClass, Object instance){
            if (expected == actual) {
                if (!expectedClass.isInstance(instance))
                    throw new IllegalRawPutException(this, actual);
                return true;
            }
            return false;
        }

        StateType(AkType... allowedTypes) {
            allowed = EnumSet.noneOf(AkType.class);
            Collections.addAll(allowed, allowedTypes);
        }

        private final Set<AkType> allowed;
    }

    public static class IllegalRawPutException extends RuntimeException {
        private IllegalRawPutException(StateType requiredStateType, AkType seenType) {
            super("illegal put of " + seenType + " to " + requiredStateType);
        }

        private IllegalRawPutException(String required) {
            super("illegal put of " + required);
        }
    }
}
