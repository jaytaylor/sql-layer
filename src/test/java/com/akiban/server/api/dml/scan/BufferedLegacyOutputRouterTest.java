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

package com.akiban.server.api.dml.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.akiban.server.error.RowOutputException;

public final class BufferedLegacyOutputRouterTest {
    private final static int bytesPerInt = Integer.SIZE / 8;

    private static class IntegersSeeingHandler implements BufferedLegacyOutputRouter.Handler {
        private final List<Integer> integers = new ArrayList<Integer>();

        @Override
        public void handleRow(byte[] bytes, int offset, int length) {
            if ( (length % bytesPerInt) != 0) {
                throw new RowOutputException(length);
            }
            ByteBuffer wrap = ByteBuffer.wrap(bytes, offset, length);
            for (int i=0; i < length / bytesPerInt; ++i) {
                integers.add( wrap.getInt() );
            }
        }

        @Override
        public void mark() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rewind() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void withResettingPosition() throws Exception {
        List<Integer> expectedInts = Arrays.asList(27, 23, 8);
        // each int is one row. If we construct this router without reset, it'll overflow
        BufferedLegacyOutputRouter router = new BufferedLegacyOutputRouter(bytesPerInt, true);
        IntegersSeeingHandler h1 = router.addHandler( new IntegersSeeingHandler() );
        IntegersSeeingHandler h2 = router.addHandler( new IntegersSeeingHandler() );

        for (Integer i : expectedInts) {
            router.getOutputBuffer().putInt(i);
            router.wroteRow(false);
        }

        assertEquals("h1", expectedInts, h1.integers);
        assertEquals("h2", expectedInts, h2.integers);
        assertEquals("rows", expectedInts.size(), router.getRowsCount());
    }

    @Test
    public void withoutResettingPosition() throws Exception {
        List<Integer> expectedInts = Arrays.asList(27, 23, 8);
        ByteBuffer actualBuffer = ByteBuffer.allocate(bytesPerInt * expectedInts.size());
        // each int is one row. If we construct this router without reset, it'll overflow
        BufferedLegacyOutputRouter router = new BufferedLegacyOutputRouter(actualBuffer, false);
        IntegersSeeingHandler h1 = router.addHandler( new IntegersSeeingHandler() );
        IntegersSeeingHandler h2 = router.addHandler( new IntegersSeeingHandler() );

        assertSame("buffer", actualBuffer, router.getOutputBuffer());

        ByteBuffer expectedBuffer = ByteBuffer.allocate(bytesPerInt * expectedInts.size());
        for (Integer i : expectedInts) {
            expectedBuffer.putInt(i);
            router.getOutputBuffer().putInt(i);
            router.wroteRow(false);
        }

        assertEquals("h1", expectedInts, h1.integers);
        assertEquals("h2", expectedInts, h2.integers);
        assertEquals("rows", expectedInts.size(), router.getRowsCount());

        assertEquals("bytes", Arrays.toString(expectedBuffer.array()), Arrays.toString(actualBuffer.array()));
    }
}
