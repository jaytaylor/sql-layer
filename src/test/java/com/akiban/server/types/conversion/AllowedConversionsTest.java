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
    public static Collection<Parameterization> params() {
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
        this.source = new AlwaysWorkingSource(sourceType);
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
            return BigDecimal.ONE;
        }

        @Override
        public BigInteger getUBigInt() {
            return BigInteger.ONE;
        }

        @Override
        public ByteSource getVarBinary() {
            return new WrappingByteSource(new byte[1]);
        }

        @Override
        public double getDouble() {
            return 0;
        }

        @Override
        public double getUDouble() {
            return 0;
        }

        @Override
        public float getFloat() {
            return 0;
        }

        @Override
        public float getUFloat() {
            return 0;
        }

        @Override
        public long getDate() {
            return 0;
        }

        @Override
        public long getDateTime() {
            return 0;
        }

        @Override
        public long getInt() {
            return 0;
        }

        @Override
        public long getLong() {
            return 0;
        }

        @Override
        public long getTime() {
            return 0;
        }

        @Override
        public long getTimestamp() {
            return 0;
        }

        @Override
        public long getUInt() {
            return 0;
        }

        @Override
        public long getYear() {
            return 0;
        }

        @Override
        public String getString() {
            return "ski";
        }

        @Override
        public String getText() {
            return "snowboard";
        }

        @Override
        public void appendAsString(AkibanAppender appender, Quote quote) {
            appender.append("ski");
        }

        @Override
        public AkType getConversionType() {
            return akType;
        }

        private AlwaysWorkingSource(AkType akType) {
            this.akType = akType;
        }

        private final AkType akType;
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
        public AkType getConversionType() {
            return akType;
        }
        
        private final AkType akType;

        private BlackHoleTarget(AkType akType) {
            this.akType = akType;
        }
    }
}
