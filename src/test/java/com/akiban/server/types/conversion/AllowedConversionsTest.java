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

package com.akiban.server.types.conversion;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.qp.operator.Cursor;
import com.akiban.server.Quote;
import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueTarget;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public final class AllowedConversionsTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws ClassNotFoundException {
        Class.forName(Converters.class.getCanonicalName()); // load the converters class (in case there are errors)
        ParameterizationBuilder builder = new ParameterizationBuilder();

        for (AkType source : AkType.values()) {
            for (AkType target : AkType.values()) {
                builder.add(source + " -> " + target, source, target);
            }
        }

        return builder.asList();
    }

    @Test @OnlyIf("conversionAllowed()")
    public void conversionWorks() {
        Converters.convert(source, target);
    }

    @Test(expected=InconvertibleTypesException.class) @OnlyIfNot("conversionAllowed()")
    public void conversionFails() {
        Converters.convert(source, target);
    }

    public AllowedConversionsTest(AkType sourceType, AkType targetType) {
        this.source = new AlwaysWorkingSource(sourceType, targetType);
        this.target = new BlackHoleTarget(targetType);
    }

    public boolean conversionAllowed() {
        return Converters.isConversionAllowed(source.getConversionType(), target.getConversionType());
    }

    private final ValueSource source;
    private final ValueTarget target;

    // nested classes

    private static class AlwaysWorkingSource implements ValueSource {
        
        @Override
        public boolean isNull() {
            return akType == AkType.NULL;
        }

        @Override
        public BigDecimal getDecimal() {
            checkType(AkType.DECIMAL);
            return BigDecimal.ONE;
        }

        @Override
        public BigInteger getUBigInt() {
            checkType(AkType.U_BIGINT);
            return BigInteger.ONE;
        }

        @Override
        public ByteSource getVarBinary() {
            checkType(AkType.VARBINARY);
            return new WrappingByteSource(new byte[1]);
        }

        @Override
        public double getDouble() {
            checkType(AkType.DOUBLE);
            return 0;
        }

        @Override
        public double getUDouble() {
            checkType(AkType.U_DOUBLE);
            return 0;
        }

        @Override
        public float getFloat() {
            checkType(AkType.FLOAT);
            return 0;
        }

        @Override
        public float getUFloat() {
            checkType(AkType.U_FLOAT);
            return 0;
        }

        @Override
        public long getDate() {
            checkType(AkType.DATE);
            return 0;
        }

        @Override
        public long getDateTime() {
            checkType(AkType.DATETIME);
            return 0;
        }

        @Override
        public long getInterval_Millis()
        {
            checkType(AkType.INTERVAL_MILLIS);
            return 0;
        }

        @Override
        public long getInterval_Month() {
            checkType(AkType.INTERVAL_MONTH);
            return 0;
        }
        
        @Override
        public long getInt() {
            checkType(AkType.INT);
            return 0;
        }

        @Override
        public long getLong() {
            checkType(AkType.LONG);
            return 0;
        }

        @Override
        public long getTime() {
            checkType(AkType.TIME);
            return 0;
        }

        @Override
        public long getTimestamp() {
            checkType(AkType.TIMESTAMP);
            return 0;
        }

        @Override
        public long getUInt() {
            checkType(AkType.U_INT);
            return 0;
        }

        @Override
        public long getYear() {
            checkType(AkType.YEAR);
            return 0;
        }

        @Override
        public String getString() {
            checkType(AkType.VARCHAR);
            return stringValue;
        }

        @Override
        public String getText() {
            checkType(AkType.TEXT);
            return stringValue;
        }

        @Override
        public void appendAsString(AkibanAppender appender, Quote quote) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AkType getConversionType() {
            return akType;
        }

        @Override
        public boolean getBool() {
            checkType(AkType.BOOL);
            return false;
        }

        @Override
        public Cursor getResultSet() {
            checkType(AkType.RESULT_SET);
            return null;
        }
        
        private AlwaysWorkingSource(AkType akType, AkType targetType) {
            this.akType = akType;
            switch (targetType) {
            case DATE:
                stringValue = "2001-01-01";
                break;
            case DATETIME:
                stringValue = "2002-02-02 22:22:22";
                break;
            case TIME:
                stringValue = "33:33:33";
                break;
            case TIMESTAMP:
                stringValue = "2003-03-03 33:33:33";
                break;
            case YEAR:
                stringValue = "2004";
                break;
            case INTERVAL_MILLIS:
                stringValue = "1234567";
                break;
            case INTERVAL_MONTH:
                stringValue = "123";
                break;
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case U_BIGINT:
            case U_DOUBLE:
            case U_FLOAT:
            case U_INT:
                stringValue = "1234";
                break;
            default:
                stringValue = null;
                break;
            }
        }

        private void checkType(AkType expected) {
            ValueSourceHelper.checkType(expected, akType);
        }

        private final AkType akType;
        private final String stringValue;
    }
    
    private static class BlackHoleTarget implements ValueTarget {
        @Override
        public void putNull() {
        }

        @Override
        public void putDate(long value) {
        }

        @Override
        public void putDateTime(long value) {
        }

        @Override
        public void putDecimal(BigDecimal value) {
        }

        @Override
        public void putDouble(double value) {
        }

        @Override
        public void putFloat(float value) {
        }

        @Override
        public void putInt(long value) {
        }

        @Override
        public void putLong(long value) {
        }

        @Override
        public void putString(String value) {
        }

        @Override
        public void putText(String value) {
        }

        @Override
        public void putTime(long value) {
        }

        @Override
        public void putTimestamp(long value) {
        }

        @Override
        public void putInterval_Millis(long value) {
        }

        @Override
        public void putInterval_Month(long value) {
        }

        @Override
        public void putUBigInt(BigInteger value) {
        }

        @Override
        public void putUDouble(double value) {
        }

        @Override
        public void putUFloat(float value) {
        }

        @Override
        public void putUInt(long value) {
        }

        @Override
        public void putVarBinary(ByteSource value) {
        }

        @Override
        public void putYear(long value) {
        }

        @Override
        public void putBool(boolean value) {
        }

        @Override
        public void putResultSet(Cursor value) {
        }

        @Override
        public AkType getConversionType() {
            return akType;
        }
        
        private final AkType akType;

        private BlackHoleTarget(AkType akType) {
            this.akType = akType;
        }
    }
}
