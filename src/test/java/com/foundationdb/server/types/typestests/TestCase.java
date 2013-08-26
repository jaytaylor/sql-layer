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

package com.foundationdb.server.types.typestests;

import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.aksql.aktypes.AkBool;
import com.foundationdb.server.types3.aksql.aktypes.AkInterval;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.mcompat.mtypes.MBinary;
import com.foundationdb.server.types3.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.Undef;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import static com.foundationdb.server.types.typestests.TestCase.TestCaseType.*;

import static org.junit.Assert.assertEquals;

public final class TestCase<T> {

    public static <T> ConversionSuite<T> suite(LinkedConversion<? super T> conversion, TestCase<? extends T>... testCases) {
        return new ConversionSuite<>(conversion, Arrays.asList(testCases));
    }

    public static <T> TestCase<T> forDate(long value, T expectedState) {
        return new TestCase<>(MDatetimes.DATE.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forDateTime(long value, T expectedState) {
        return new TestCase<>(MDatetimes.DATETIME.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forDecimal(BigDecimal value, long precision, long scale, T expectedState) {
        return new TestCase<>(MNumeric.DECIMAL.instance(true), value, TC_OBJECT, precision, scale, expectedState);
    }

    public static <T> TestCase<T> forDouble(double value, T expectedState) {
        return new TestCase<>(MApproximateNumber.DOUBLE.instance(true), value, TC_DOUBLE, expectedState);
    }

    public static <T> TestCase<T> forFloat(float value, T expectedState) {
        return new TestCase<>(MApproximateNumber.FLOAT.instance(true), value, TC_FLOAT, expectedState);
    }

    public static <T> TestCase<T> forTinyInt (long value, T expectedState) {
        return new TestCase<>(MNumeric.TINYINT.instance(true), value, TC_LONG, expectedState);
    }
    
    public static <T> TestCase<T> forInt(long value, T expectedState) {
        return new TestCase<>(MNumeric.INT.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forLong(long value, T expectedState) {
        return new TestCase<>(MNumeric.BIGINT.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forChar (char value, T expectedState) {
        return new TestCase<>(MNumeric.MEDIUMINT_UNSIGNED.instance(true), value, TC_LONG, expectedState);
    }
    
    public static <T> TestCase<T> forString(String value, long maxWidth, String charset, T expectedState) {
        return new TestCase<>(MString.VARCHAR.instance(true), value, TC_OBJECT, maxWidth, charset, expectedState);
    }

    public static <T> TestCase<T> forText(String value, long maxWidth, String charset, T expectedState) {
        return new TestCase<>(MString.TEXT.instance(true), value, TC_OBJECT, maxWidth, charset, expectedState);
    }

    public static <T> TestCase<T> forTime(long value, T expectedState) {
        return new TestCase<>(MDatetimes.TIME.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forTimestamp(long value, T expectedState) {
        return new TestCase<>(MDatetimes.TIMESTAMP.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forInterval_Millis(long value, T expectedState) {
        return new TestCase<>(AkInterval.SECONDS.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forInterval_Month(long value, T expectedState) {
        return new TestCase<>(AkInterval.MONTHS.instance(true), value, TC_LONG, expectedState);
    }
    public static <T> TestCase<T> forUBigInt(BigInteger value, T expectedState) {
        return new TestCase<>(MNumeric.BIGINT_UNSIGNED.instance(true), value, TC_OBJECT, expectedState);
    }

    public static <T> TestCase<T> forUDouble(double value, T expectedState) {
        return new TestCase<>(MApproximateNumber.DOUBLE_UNSIGNED.instance(true), value, TC_DOUBLE, expectedState);
    }

    public static <T> TestCase<T> forUFloat(float value, T expectedState) {
        return new TestCase<>(MApproximateNumber.FLOAT_UNSIGNED.instance(true), value, TC_FLOAT, expectedState);
    }

    public static <T> TestCase<T> forUInt(long value, T expectedState) {
        return new TestCase<>(MNumeric.INT_UNSIGNED.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forVarBinary(ByteSource value, long maxWidth, T expectedState) {
        return new TestCase<>(MBinary.VARBINARY.instance(true), value, TC_OBJECT, maxWidth, expectedState);
    }

    public static <T> TestCase<T> forYear(long value, T expectedState) {
        return new TestCase<>(MDatetimes.YEAR.instance(true), value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forBool(boolean value, T expectedState) {
        return new TestCase<>(AkBool.INSTANCE.instance(true), bool2long(value), TC_LONG, expectedState);
    }

    static <T> TestCase<T> derive(TestCase<?> source, T newState) {
        return new TestCase<>(source, newState);
    }

    public TInstance type() {
        return tInstance;
    }

    public Long param1() {
        return param1;
    }

    public Long param2() {
        return param2;
    }

    public String charset() {
        return charset;
    }

    public void put(PValueTarget target) {
        if (TClass.comparisonNeedsCasting(target.tInstance(), tInstance)) {
            throw new WrongValueGetException (target.tInstance(), tInstance);
        }
        switch (TInstance.pUnderlying(target.tInstance())) {
        case BOOL:
            target.putBool(long2bool(valLong)); break;
        case BYTES:
            target.putBytes(((ByteSource)valObject).byteArray()); break;
        case DOUBLE:
            target.putDouble(valDouble);
            break;
        case FLOAT:
            target.putFloat(valFloat);
            break;
        case INT_16:
            target.putInt16((short)valLong);
            break;
        case INT_32:
            target.putInt32((int)valLong);
            break;
        case INT_64:
            target.putInt64(valLong);
            break;
        case INT_8:
            target.putInt8((byte)valLong);
            break;
        case STRING:
            target.putString((String)valObject, null);
            break;
        case UINT_16:
            target.putUInt16((char)valLong);
            break;
        default:
            throw new UnsupportedOperationException(type().toString());
        }
    }

    // for use in this package

    void check(PValueSource source) {
        switch (TInstance.pUnderlying(source.tInstance())) {
        case BOOL:
            assertEquals(niceString(), long2bool(valLong), source.getBoolean());
            break;
        case BYTES:
            assertEquals(niceString(), ((ByteSource)valObject).byteArray(), source.getBytes());
            break;
        case DOUBLE:
            assertEquals(niceString(), valDouble, source.getDouble(), EPSILON);
            break;
        case FLOAT:
            assertEquals(niceString(), valFloat, source.getFloat(), EPSILON);
            break;
        case INT_16:
            assertEquals(niceString(), valLong, source.getInt16());
            break;
        case INT_32:
            assertEquals(niceString(), valLong, source.getInt32());
            break;
        case INT_64:
            assertEquals(niceString(), valLong, source.getInt64());
            break;
        case INT_8:
            assertEquals(niceString(), valLong, source.getInt8());
            break;
        case STRING:
            assertEquals(niceString(), (String)valObject, source.getString());
            break;
        case UINT_16:
            assertEquals(niceString(), valLong, source.getUInt16());
            break;
        default:
            throw new UnsupportedOperationException(type().toString());
        }
    }

    private String niceString() {
        String result = tInstance.toString();
        result += niceLongValue();
        return result;
    }

    private String niceLongValue() {
        return "(" + valLong + "->\"" + valLong +"\")";
    }

    private static long bool2long(boolean value) {
        return value ? TRUE_LONG : FALSE_LONG;
    }

    private static boolean long2bool(long value) {
        return value != 0;
    }

    void get(PValueSource source) {
        if (TClass.comparisonNeedsCasting(source.tInstance(), tInstance)) {
            throw new WrongValueGetException (source.tInstance(), tInstance);
        }
    }
    
    T expectedState() {
        return expectedState;
    }
    
    // Object interface

    @Override
    public String toString() {
        final Object value;
        if (testCaseType == null) {
            value = "<NULL TEST CASE TYPE>";
        }
        else {
            switch (testCaseType) {
            case TC_FLOAT:
                value = valFloat;
                break;
            case TC_DOUBLE:
                value = valDouble;
                break;
            case TC_LONG:
                value = niceLongValue();
                break;
            case TC_OBJECT:
                value = valObject;
                break;
            default:
                value = "<UNKNOWN TEST CASE TYPE: " + testCaseType + '>';
                break;
            }
        }
        return String.format("TestCase(%s = %s: %s)", tInstance.toString(), value, expectedState == NO_STATE ? "std" : expectedState);
    }


    // for use in this class

    private TestCase(TInstance tInstance, double value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, tInstance, value, NO_FLOAT, NO_LONG, NO_OBJECT, null, null, null, expectedState);
        checkTestCaseType(TC_DOUBLE, testCaseType);
    }
    private TestCase(TInstance tInstance, float value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, tInstance, NO_DOUBLE, value, NO_LONG, NO_OBJECT, null, null, null, expectedState);
        checkTestCaseType(TC_FLOAT, testCaseType);
    }
    private TestCase(TInstance tInstance, long value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, tInstance, NO_DOUBLE, NO_FLOAT, value, NO_OBJECT, null, null, null, expectedState);
        checkTestCaseType(TC_LONG, testCaseType);
    }
    private TestCase(TInstance tInstance, Object value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, tInstance, NO_DOUBLE, NO_FLOAT, NO_LONG, value, null, null, null, expectedState);
        checkTestCaseType(TC_OBJECT, testCaseType);
    }
    private TestCase(TInstance tInstance, Object value, TestCaseType testCaseType, long param1, T expectedState) {
        this(testCaseType, tInstance, NO_DOUBLE, NO_FLOAT, NO_LONG, value, param1, null, null, expectedState);
        checkTestCaseType(TC_OBJECT, testCaseType);
    }
    private TestCase(TInstance tInstance, Object value, TestCaseType testCaseType, long param1, long param2, T expectedState) {
        this(testCaseType, tInstance, NO_DOUBLE, NO_FLOAT, NO_LONG, value, param1, param2, null, expectedState);
        checkTestCaseType(TC_OBJECT, testCaseType);
    }
    private TestCase(TInstance tInstance, Object value, TestCaseType testCaseType, long param1, String charset, T expectedState) {
        this(testCaseType, tInstance, NO_DOUBLE, NO_FLOAT, NO_LONG, value, param1, null, charset, expectedState);
        checkTestCaseType(TC_OBJECT, testCaseType);
    }

    public TestCase(TestCase<?> source, T newState) {
        this(
                source.testCaseType,
                source.tInstance,
                source.valDouble,
                source.valFloat,
                source.valLong,
                source.valObject,
                source.param1,
                source.param2,
                source.charset,
                newState
        );
    }

    private TestCase(TestCaseType tct,
                     TInstance tinstance, double valDouble, float valFloat, long valLong, Object valObject,
                     Long param1, Long param2, String charset,
                     T expectedState) {
        this.testCaseType = tct;
        this.tInstance = tinstance;
        //this.type = type;
        this.valDouble = valDouble;
        this.valFloat = valFloat;
        this.valLong = valLong;
        this.valObject = valObject;
        this.expectedState = expectedState;
        this.param1 = param1;
        this.param2 = param2;
        this.charset = charset;
    }

    private static void checkTestCaseType(TestCaseType expected, TestCaseType actual) {
        assertEquals("test case type", expected, actual);
    }

    
    
    // Object state
    private final TestCaseType testCaseType;
    private final TInstance tInstance;
    //private final AkType type;
    private final double valDouble;
    private final float valFloat;
    private final long valLong;
    private final Object valObject;
    private final T expectedState;
    private final Long param1;
    private final Long param2;
    private final String charset;

    // consts
    static final Object NO_STATE = new Object();

    private static final double EPSILON = 0;
    private static final double NO_DOUBLE = -1;
    private static final float NO_FLOAT = -1;
    private static final long NO_LONG = -1;
    private static final Object NO_OBJECT = Undef.only();
    private static final long TRUE_LONG = 1;
    private static final long FALSE_LONG = 0;

    // nested classes
    enum TestCaseType {
        TC_FLOAT,
        TC_DOUBLE,
        TC_LONG,
        TC_OBJECT
    }
}
