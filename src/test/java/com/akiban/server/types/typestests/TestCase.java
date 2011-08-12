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

package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
import com.akiban.util.ByteSource;
import com.akiban.util.Undef;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import static com.akiban.server.types.AkType.*;
import static com.akiban.server.types.typestests.TestCase.TestCaseType.*;

import static org.junit.Assert.assertEquals;

public final class TestCase<T> {

    public static <T> ConversionSuite<T> suite(LinkedConversion<? super T> conversion, TestCase<? extends T>... testCases) {
        return new ConversionSuite<T>(conversion, Arrays.asList(testCases));
    }

    public static <T> TestCase<T> forDate(long value, T expectedState) {
        return new TestCase<T>(DATE, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forDateTime(long value, T expectedState) {
        return new TestCase<T>(DATETIME, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forDecimal(BigDecimal value, T expectedState) {
        return new TestCase<T>(DECIMAL, value, TC_OBJECT, expectedState);
    }

    public static <T> TestCase<T> forDouble(double value, T expectedState) {
        return new TestCase<T>(DOUBLE, value, TC_DOUBLE, expectedState);
    }

    public static <T> TestCase<T> forFloat(float value, T expectedState) {
        return new TestCase<T>(FLOAT, value, TC_FLOAT, expectedState);
    }

    public static <T> TestCase<T> forInt(long value, T expectedState) {
        return new TestCase<T>(INT, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forLong(long value, T expectedState) {
        return new TestCase<T>(LONG, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forString(String value, T expectedState) {
        return new TestCase<T>(VARCHAR, value, TC_OBJECT, expectedState);
    }

    public static <T> TestCase<T> forText(String value, T expectedState) {
        return new TestCase<T>(TEXT, value, TC_OBJECT, expectedState);
    }

    public static <T> TestCase<T> forTime(long value, T expectedState) {
        return new TestCase<T>(TIME, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forTimestamp(long value, T expectedState) {
        return new TestCase<T>(TIMESTAMP, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forUBigInt(BigInteger value, T expectedState) {
        return new TestCase<T>(U_BIGINT, value, TC_OBJECT, expectedState);
    }

    public static <T> TestCase<T> forUDouble(double value, T expectedState) {
        return new TestCase<T>(U_DOUBLE, value, TC_DOUBLE, expectedState);
    }

    public static <T> TestCase<T> forUFloat(float value, T expectedState) {
        return new TestCase<T>(U_FLOAT, value, TC_FLOAT, expectedState);
    }

    public static <T> TestCase<T> forUInt(long value, T expectedState) {
        return new TestCase<T>(U_INT, value, TC_LONG, expectedState);
    }

    public static <T> TestCase<T> forVarBinary(ByteSource value, T expectedState) {
        return new TestCase<T>(VARBINARY, value, TC_OBJECT, expectedState);
    }

    public static <T> TestCase<T> forYear(long value, T expectedState) {
        return new TestCase<T>(YEAR, value, TC_LONG, expectedState);
    }

    static <T> TestCase<T> derive(TestCase<?> source, T newState) {
        return new TestCase<T>(source, newState);
    }

    // for use in this package

    void put(ConversionTarget target) {
        switch (type) {
        case DATE: target.putDate(valLong); break;
        case DATETIME: target.putDateTime(valLong); break;
        case DECIMAL: target.putDecimal((BigDecimal)valObject); break;
        case DOUBLE: target.putDouble(valDouble); break;
        case FLOAT: target.putFloat(valFloat); break;
        case INT: target.putInt(valLong); break;
        case LONG: target.putLong(valLong); break;
        case VARCHAR: target.putString((String)valObject); break;
        case TEXT: target.putText((String)valObject); break;
        case TIME: target.putTime(valLong); break;
        case TIMESTAMP: target.putTimestamp(valLong); break;
        case U_BIGINT: target.putUBigInt((BigInteger)valObject); break;
        case U_DOUBLE: target.putUDouble(valDouble); break;
        case U_FLOAT: target.putUFloat(valFloat); break;
        case U_INT: target.putUInt(valLong); break;
        case VARBINARY: target.putVarBinary((ByteSource)valObject); break;
        case YEAR: target.putYear(valLong); break;
        default: throw new UnsupportedOperationException(type().name());
        }
    }

    void check(ConversionSource source) {
        switch (type) {
        case DATE: assertEquals(type.name(), valLong, source.getDate()); break;
        case DATETIME: assertEquals(type.name(), valLong, source.getDateTime()); break;
        case DECIMAL: assertEquals(type.name(), valObject, source.getDecimal()); break;
        case DOUBLE: assertEquals(type.name(), valDouble, source.getDouble(), EPSILON); break;
        case FLOAT: assertEquals(type.name(), valFloat, source.getFloat(), EPSILON); break;
        case INT: assertEquals(type.name(), valLong, source.getInt()); break;
        case LONG: assertEquals(type.name(), valLong, source.getLong()); break;
        case VARCHAR: assertEquals(type.name(), valObject, source.getString()); break;
        case TEXT: assertEquals(type.name(), valObject, source.getText()); break;
        case TIME: assertEquals(type.name(), valLong, source.getTime()); break;
        case TIMESTAMP: assertEquals(type.name(), valLong, source.getTimestamp()); break;
        case U_BIGINT: assertEquals(type.name(), valObject, source.getUBigInt()); break;
        case U_DOUBLE: assertEquals(type.name(), valDouble, source.getUDouble(), EPSILON); break;
        case U_FLOAT: assertEquals(type.name(), valFloat, source.getUFloat(), EPSILON); break;
        case U_INT: assertEquals(type.name(), valLong, source.getUInt()); break;
        case VARBINARY: assertEquals(type.name(), valObject, source.getVarBinary()); break;
        case YEAR: assertEquals(type.name(), valLong, source.getYear()); break;
        default: throw new UnsupportedOperationException(type().name());
        }
    }

    void get(ConversionSource source) {
        switch (type) {
        case DATE: source.getDate(); break;
        case DATETIME: source.getDateTime(); break;
        case DECIMAL: source.getDecimal(); break;
        case DOUBLE: source.getDouble(); break;
        case FLOAT: source.getFloat(); break;
        case INT: source.getInt(); break;
        case LONG: source.getLong(); break;
        case VARCHAR: source.getString(); break;
        case TEXT: source.getText(); break;
        case TIME: source.getTime(); break;
        case TIMESTAMP: source.getTimestamp(); break;
        case U_BIGINT: source.getUBigInt(); break;
        case U_DOUBLE: source.getUDouble(); break;
        case U_FLOAT: source.getUFloat(); break;
        case U_INT: source.getUInt(); break;
        case VARBINARY: source.getVarBinary(); break;
        case YEAR: source.getYear(); break;
        default: throw new UnsupportedOperationException(type().name());
        }
    }
    
    T expectedState() {
        return expectedState;
    }

    AkType type() {
        return type;
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
                value = valLong;
                break;
            case TC_OBJECT:
                value = valObject;
                break;
            default:
                value = "<UNKNOWN TEST CASE TYPE: " + testCaseType + '>';
                break;
            }
        }
        return String.format("TestCase(%s = %s: %s)", type, value, expectedState == NO_STATE ? "std" : expectedState);
    }


    // for use in this class

    private TestCase(AkType type, double value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, type, value, NO_FLOAT, NO_LONG, NO_OBJECT, expectedState);
        checkTestCaseType(TC_DOUBLE, testCaseType);
    }
    private TestCase(AkType type, float value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, type, NO_DOUBLE, value, NO_LONG, NO_OBJECT, expectedState);
        checkTestCaseType(TC_FLOAT, testCaseType);
    }
    private TestCase(AkType type, long value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, type, NO_DOUBLE, NO_FLOAT, value, NO_OBJECT, expectedState);
        checkTestCaseType(TC_LONG, testCaseType);
    }
    private TestCase(AkType type, Object value, TestCaseType testCaseType, T expectedState) {
        this(testCaseType, type, NO_DOUBLE, NO_FLOAT, NO_LONG, value, expectedState);
        checkTestCaseType(TC_OBJECT, testCaseType);
    }

    public TestCase(TestCase<?> source, T newState) {
        this(
                source.testCaseType,
                source.type,
                source.valDouble,
                source.valFloat,
                source.valLong,
                source.valObject,
                newState
        );
    }

    private TestCase(TestCaseType tct, AkType type, double valDouble, float valFloat, long valLong, Object valObject, T expectedState) {
        this.testCaseType = tct;
        this.type = type;
        this.valDouble = valDouble;
        this.valFloat = valFloat;
        this.valLong = valLong;
        this.valObject = valObject;
        this.expectedState = expectedState;
    }

    private static void checkTestCaseType(TestCaseType expected, TestCaseType actual) {
        assertEquals("test case type", expected, actual);
    }

    // Object state
    private final TestCaseType testCaseType;
    private final AkType type;
    private final double valDouble;
    private final float valFloat;
    private final long valLong;
    private final Object valObject;
    private final T expectedState;

    // consts
    static final Object NO_STATE = new Object();

    private static final double EPSILON = 0;
    private static final double NO_DOUBLE = -1;
    private static final float NO_FLOAT = -1;
    private static final long NO_LONG = -1;
    private static final Object NO_OBJECT = Undef.only();

    // nested classes
    enum TestCaseType {
        TC_FLOAT,
        TC_DOUBLE,
        TC_LONG,
        TC_OBJECT
    }
}
