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
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public final class ColumnSetTest {

    @Test
    public void pack1Byte() throws Exception {
        Set<Integer> columns = new HashSet<>();
        columns.add( 0 );
        columns.add( 6 );

        assertBytes("[ 10000010 ]", columns);
    }

    @Test
    public void pack2BytesBoth() throws Exception {
        Set<Integer> columns = new HashSet<>();
        columns.add( 0 );
        columns.add( 6 );
        columns.add( 9 );

        assertBytes("[ 10000010 01000000 ]", columns);
    }

    @Test
    public void pack2BytesSparse() throws Exception {
        Set<Integer> columns = new HashSet<>();
        columns.add( 8 );

        assertBytes("[ 00000000 10000000 ]", columns);
    }

    @Test
    public void emptySet() {
        Set<Integer> empty = new HashSet<>();
        assertNotNull("got null byte[]", ColumnSet.packToLegacy(empty));
        assertBytes("[ ]", empty);
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

    @Test
    public void handleEighthBitSet() {
        final Set<Integer> columns = new HashSet<>();
        columns.addAll(Arrays.asList(0,7,8));
        assertBytes("[ 10000001 10000000 ]", columns);
    }

    @Test
    public void threeByteWrongOutput() {
        final Set<Integer> columnsFitsTwo = new HashSet<>();
        columnsFitsTwo.addAll(Arrays.asList(0,1,2,3,4,5,10,11,15));
        assertBytes("[ 11111100 00110001 ]", columnsFitsTwo);

        final Set<Integer> columnsFitsThree = new HashSet<>();
        columnsFitsThree.addAll(Arrays.asList(0,1,2,3,4,5,10,11,15,16));
        assertBytes("[ 11111100 00110001 10000000 ]", columnsFitsThree);

        // The second byte was getting lost internally to packToLegacy (turned out to be a ByteBuffer copy error)
        final Set<Integer> columnsActualCase = new HashSet<>();
        columnsActualCase.addAll(Arrays.asList(0,1,2,3,4,10,11,15,16,17,18,19,20,21,22,23));
        assertBytes("[ 11111000 00110001 11111111 ]", columnsActualCase);
    }
    
    private static void assertBytes(String expected, Set<Integer> actual) {
        final byte[] actualBytes =  ColumnSet.packToLegacy(actual);
        assertEquals("bytes", expected, bytesToHex(actualBytes));

        Set<Integer> unpacked = ColumnSet.unpackFromLegacy(actualBytes);
        assertEquals("unpacked set", actual, unpacked);
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
