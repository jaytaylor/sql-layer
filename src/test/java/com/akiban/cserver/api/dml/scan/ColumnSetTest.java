package com.akiban.cserver.api.dml.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.akiban.cserver.api.common.ColumnId;

public final class ColumnSetTest {

    @Test
    public void pack1Byte() throws Exception {
        Set<ColumnId> columns = new HashSet<ColumnId>();
        columns.add( ColumnId.of(0) );
        columns.add( ColumnId.of(6) );

        assertBytes("[ 10000010 ]", columns);
    }

    @Test
    public void pack2BytesBoth() throws Exception {
        Set<ColumnId> columns = new HashSet<ColumnId>();
        columns.add( ColumnId.of(0) );
        columns.add( ColumnId.of(6) );
        columns.add( ColumnId.of(9) );

        assertBytes("[ 10000010 01000000 ]", columns);
    }

    @Test
    public void pack2BytesSparse() throws Exception {
        Set<ColumnId> columns = new HashSet<ColumnId>();
        columns.add( ColumnId.of(8) );

        assertBytes("[ 00000000 10000000 ]", columns);
    }

    @Test
    public void emptySet() {
        Set<ColumnId> empty = new HashSet<ColumnId>();
        assertNotNull("got null byte[]", ColumnSet.packToLegacy(empty));
        assertBytes("[ ]", empty);
    }

    private static void assertBytes(String expected, Set<ColumnId> actual) {
        final byte[] actualBytes =  ColumnSet.packToLegacy(actual);
        assertEquals("bytes", expected, bytesToHex(actualBytes));

        Set<ColumnId> unpacked = ColumnSet.unpackFromLegacy(actualBytes);
        assertEquals("unpacked set", actual, unpacked);
    }

    @Test
    public void testBytesToHex() {
        assertEquals("empty array", "[ ]", bytesToHex(new byte[] {} ));

        assertEquals("zero array", "[ 00000000 ]", bytesToHex(new byte[] {0} ));
        assertEquals("one array", "[ 10000000 ]", bytesToHex(new byte[] {1} ));
        assertEquals("one byte array", "[ 00010000 ]", bytesToHex(new byte[] {8} ));
        assertEquals("one byte array", "[ 11100000 10100000 ]", bytesToHex(new byte[] {7, 5} ));
        assertEquals("one byte array", "[ 00000000 10000000 ]", bytesToHex(new byte[] {(byte)0, 1} ));

    }

    private static String bytesToHex(byte[] actualBytes) {
        StringBuilder builder = new StringBuilder(4 + actualBytes.length*9 );
        builder.append("[ ");

        for(byte theByte : actualBytes) {
            for (int i=1; i <= 128; i <<= 1) {
                builder.append( (theByte & i) == i ? '1' : '0');
            }
            builder.append(' ');
        }

        builder.append(']');
        return builder.toString();
    }
}
