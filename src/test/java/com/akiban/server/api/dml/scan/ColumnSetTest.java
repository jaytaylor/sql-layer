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
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public final class ColumnSetTest {

    @Test
    public void pack1Byte() throws Exception {
        Set<Integer> columns = new HashSet<Integer>();
        columns.add( 0 );
        columns.add( 6 );

        assertBytes("[ 10000010 ]", columns);
    }

    @Test
    public void pack2BytesBoth() throws Exception {
        Set<Integer> columns = new HashSet<Integer>();
        columns.add( 0 );
        columns.add( 6 );
        columns.add( 9 );

        assertBytes("[ 10000010 01000000 ]", columns);
    }

    @Test
    public void pack2BytesSparse() throws Exception {
        Set<Integer> columns = new HashSet<Integer>();
        columns.add( 8 );

        assertBytes("[ 00000000 10000000 ]", columns);
    }

    @Test
    public void emptySet() {
        Set<Integer> empty = new HashSet<Integer>();
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
        final Set<Integer> columns = new HashSet<Integer>();
        columns.addAll(Arrays.asList(0,7,8));
        assertBytes("[ 10000001 10000000 ]", columns);
    }

    @Test
    public void threeByteWrongOutput() {
        final Set<Integer> columnsFitsTwo = new HashSet<Integer>();
        columnsFitsTwo.addAll(Arrays.asList(0,1,2,3,4,5,10,11,15));
        assertBytes("[ 11111100 00110001 ]", columnsFitsTwo);

        final Set<Integer> columnsFitsThree = new HashSet<Integer>();
        columnsFitsThree.addAll(Arrays.asList(0,1,2,3,4,5,10,11,15,16));
        assertBytes("[ 11111100 00110001 10000000 ]", columnsFitsThree);

        // The second byte was getting lost internally to packToLegacy (turned out to be a ByteBuffer copy error)
        final Set<Integer> columnsActualCase = new HashSet<Integer>();
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
