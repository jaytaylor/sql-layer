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

package com.akiban.util;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
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
