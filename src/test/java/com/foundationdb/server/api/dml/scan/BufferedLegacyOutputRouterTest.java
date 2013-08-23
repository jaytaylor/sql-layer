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

package com.foundationdb.server.api.dml.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.foundationdb.util.GrowableByteBuffer;
import org.junit.Test;

import com.foundationdb.server.error.RowOutputException;

public final class BufferedLegacyOutputRouterTest {
    private final static int bytesPerInt = Integer.SIZE / 8;

    private static class IntegersSeeingHandler implements BufferedLegacyOutputRouter.Handler {
        private final List<Integer> integers = new ArrayList<>();

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
        GrowableByteBuffer actualBuffer = new GrowableByteBuffer(bytesPerInt * expectedInts.size());
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
