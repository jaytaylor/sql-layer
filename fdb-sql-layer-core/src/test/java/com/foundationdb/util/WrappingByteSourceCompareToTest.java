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

package com.foundationdb.util;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class WrappingByteSourceCompareToTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        build(pb, bs(), bs(), CompareResult.EQ);
        build(pb, bs(1), bs(), CompareResult.GT);
        build(pb, bs(), bs(1), CompareResult.LT);

        build(pb, bs(5), bs(5), CompareResult.EQ);
        build(pb, bs(5, 1), bs(5), CompareResult.GT);
        build(pb, bs(5), bs(5, 1), CompareResult.LT);

        build(pb, bs(4), bs(5), CompareResult.LT);
        build(pb, bs(4, 1), bs(5), CompareResult.LT);
        build(pb, bs(4), bs(5, 1), CompareResult.LT);

        build(pb, bs(6), bs(5), CompareResult.GT);
        build(pb, bs(6, 1), bs(5), CompareResult.GT);
        build(pb, bs(6), bs(5, 1), CompareResult.GT);

        return pb.asList();
    }

    @Test
    public void normal() {
        test(new WrappingByteSource(bytesOne), new WrappingByteSource(bytesTwo));
    }

    @Test
    public void oneIsOffset() {
        test(wrapWithOffset(bytesOne), new WrappingByteSource(bytesTwo));
    }

    @Test
    public void twoIsOffset() {
        test(new WrappingByteSource(bytesOne), wrapWithOffset(bytesTwo));
    }

    private ByteSource wrapWithOffset(byte[] target) {
        byte[] withOffset = new byte[target.length + OFFSET];
        Arrays.fill(withOffset, (byte) 31);
        System.arraycopy(target, 0, withOffset, OFFSET, target.length);
        return new WrappingByteSource().wrap(withOffset, OFFSET, target.length);
    }

    public WrappingByteSourceCompareToTest(byte[] bytesOne, byte[] bytesTwo, CompareResult expected) {
        this.bytesOne = bytesOne;
        this.bytesTwo = bytesTwo;
        this.expected = expected;
    }

    private final byte[] bytesOne;
    private final byte[] bytesTwo;
    private final CompareResult expected;

    private void test(ByteSource one, ByteSource two) {
        CompareResult actual = CompareResult.fromInt(one.compareTo(two));
        assertEquals(expected, actual);
    }

    private static void build(ParameterizationBuilder pb, byte[] one, byte[] two, CompareResult expected) {
        StringBuilder sb = new StringBuilder();
        sb.append(Arrays.toString(one)).append(' ').append(expected).append(' ').append(Arrays.toString(two));
        pb.add(sb.toString(), one, two, expected);
    }

    private static byte[] bs(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i=0; i < ints.length; ++i) {
            bytes[i] = (byte)ints[i];
        }
        return bytes;
    }

    private static final int OFFSET = 4;

    private enum CompareResult {
        LT,
        EQ,
        GT
        ;

        public static CompareResult fromInt(int compareToResult) {
            if (compareToResult < 0)
                return LT;
            return (compareToResult > 0) ? GT : EQ;
        }
    }
}
