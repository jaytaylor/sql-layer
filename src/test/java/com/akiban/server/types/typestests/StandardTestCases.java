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
import com.akiban.server.types.Converters;
import com.akiban.server.types.LongConverter;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.akiban.server.types.typestests.TestCase.NO_STATE;

final class StandardTestCases {
    static Collection<TestCase<?>> get() {
        return onlyList;
    }

    private static Collection<TestCase<?>> make() {
        List<TestCase<?>> list = new ArrayList<TestCase<?>>();
        
        // TODO need to figure out upper and lower limits, test those

        list.add(TestCase.forDate(0, NO_STATE));
        list.add(TestCase.forDate(5119897L, NO_STATE)); // 9999-12-15
        
        list.add(TestCase.forDateTime(0, NO_STATE));
        list.add(TestCase.forDateTime(99991231235959L, NO_STATE));
        
        list.add(TestCase.forDecimal(BigDecimal.ZERO, NO_STATE));
        list.add(TestCase.forDecimal(BigDecimal.ONE, NO_STATE));
        list.add(TestCase.forDecimal(BigDecimal.TEN, NO_STATE));
        list.add(TestCase.forDecimal(BigDecimal.valueOf(-10), NO_STATE));
        
        list.add(TestCase.forDouble(0, NO_STATE));
        list.add(TestCase.forDouble(-0, NO_STATE));
        list.add(TestCase.forDouble(1, NO_STATE));
        list.add(TestCase.forDouble(-1, NO_STATE));
        list.add(TestCase.forDouble(Double.MIN_VALUE, NO_STATE));
        list.add(TestCase.forDouble(Double.MAX_VALUE, NO_STATE));
        
        list.add(TestCase.forFloat(0, NO_STATE));
        list.add(TestCase.forFloat(-0, NO_STATE));
        list.add(TestCase.forFloat(1, NO_STATE));
        list.add(TestCase.forFloat(-1, NO_STATE));
        list.add(TestCase.forFloat(Float.MIN_VALUE, NO_STATE));
        list.add(TestCase.forFloat(Float.MAX_VALUE, NO_STATE));

        list.add(TestCase.forInt(-1, NO_STATE));
        list.add(TestCase.forInt(0, NO_STATE));
        list.add(TestCase.forInt(1, NO_STATE));
        list.add(TestCase.forInt(-1, Long.MAX_VALUE));
        list.add(TestCase.forInt(-1, Long.MIN_VALUE));

        list.add(TestCase.forLong(-1, NO_STATE));
        list.add(TestCase.forLong(0, NO_STATE));
        list.add(TestCase.forLong(1, NO_STATE));
        list.add(TestCase.forLong(-1, Long.MAX_VALUE));
        list.add(TestCase.forLong(-1, Long.MIN_VALUE));

        list.add(TestCase.forString("", NO_STATE));
        list.add(TestCase.forString("word", NO_STATE));
        list.add(TestCase.forString("a snowman says ☃", NO_STATE));

        list.add(TestCase.forText("", NO_STATE));
        list.add(TestCase.forText("word", NO_STATE));
        list.add(TestCase.forText("a snowman says ☃", NO_STATE));

        list.add(TestCase.forTime(-1, NO_STATE));
        list.add(TestCase.forTime(0, NO_STATE));
        list.add(TestCase.forTime(1, NO_STATE));

        list.add(TestCase.forTimestamp(0, NO_STATE));
        list.add(TestCase.forTimestamp(Long.MAX_VALUE, NO_STATE));
        list.add(TestCase.forTimestamp(Long.MIN_VALUE, NO_STATE));

        list.add(TestCase.forUBigInt(BigInteger.ZERO, NO_STATE));
        list.add(TestCase.forUBigInt(BigInteger.ONE, NO_STATE));
        list.add(TestCase.forUBigInt(BigInteger.TEN, NO_STATE));
        list.add(TestCase.forUBigInt(BigInteger.valueOf(-1), NO_STATE));

        list.add(TestCase.forUDouble(0, NO_STATE));
        list.add(TestCase.forUDouble(-0, NO_STATE));
        list.add(TestCase.forUDouble(1, NO_STATE));
        list.add(TestCase.forUDouble(-1, NO_STATE));
        list.add(TestCase.forUDouble(Double.MIN_VALUE, NO_STATE));
        list.add(TestCase.forUDouble(Double.MAX_VALUE, NO_STATE));

        list.add(TestCase.forUFloat(0, NO_STATE));
        list.add(TestCase.forUFloat(-0, NO_STATE));
        list.add(TestCase.forUFloat(1, NO_STATE));
        list.add(TestCase.forUFloat(-1, NO_STATE));
        list.add(TestCase.forUFloat(Float.MIN_VALUE, NO_STATE));
        list.add(TestCase.forUFloat(Float.MAX_VALUE, NO_STATE));

        list.add(TestCase.forUInt(-1, NO_STATE));
        list.add(TestCase.forUInt(0, NO_STATE));
        list.add(TestCase.forUInt(1, NO_STATE));
        list.add(TestCase.forUInt(Long.MIN_VALUE, NO_STATE));
        list.add(TestCase.forUInt(Long.MAX_VALUE, NO_STATE));

        list.add(TestCase.forVarBinary(wrap(), NO_STATE));
        list.add(TestCase.forVarBinary(wrap(Byte.MIN_VALUE, Byte.MAX_VALUE, 0), NO_STATE));

        LongConverter yearConverter = Converters.getLongConverter(AkType.YEAR);
        list.add(TestCase.forYear(yearConverter.doParse("0000"), NO_STATE));
        list.add(TestCase.forYear(yearConverter.doParse("1901"), NO_STATE));
        list.add(TestCase.forYear(yearConverter.doParse("1983"), NO_STATE));
        list.add(TestCase.forYear(yearConverter.doParse("2155"), NO_STATE));

        return Collections.unmodifiableCollection(list);
    }

    private static ByteSource wrap(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i=0; i < bytes.length; ++i) {
            int asInt = values[i];
            if (asInt < Byte.MIN_VALUE || asInt > Byte.MAX_VALUE) {
                throw new AssertionError("invalid byte value " + asInt);
            }
            bytes[i] = (byte)asInt;
        }
        return new WrappingByteSource().wrap(bytes);
    }

    private static Collection<TestCase<?>> onlyList = make();
}
