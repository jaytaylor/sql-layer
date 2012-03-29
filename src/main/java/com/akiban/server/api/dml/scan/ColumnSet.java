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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.akiban.util.ArgumentValidation;

public final class ColumnSet {

    public static Set<Integer> ofPositions(int... positions) {
        Set<Integer> asSet = new HashSet<Integer>();
        for(int pos : positions) {
            asSet.add(pos);
        }
        return asSet;
    }

    public static int unpackByteFromLegacy(byte theByte, int byteNum, Set<Integer> out) {
        int added = 0;
        for(int relativePos=0; relativePos < 8; ++relativePos) {
            if ( 0!= (theByte & (1 << relativePos))) {
                if (out.add( (byteNum*8) + relativePos) ) {
                    ++added;
                }
            }
        }
        return added;
    }

    public static Set<Integer> unpackFromLegacy(byte[] columns) {
        ArgumentValidation.notNull("columns", columns);
        if (columns.length == 0) {
            return Collections.emptySet();
        }
        Set<Integer> retval = new HashSet<Integer>();
        for (int byteNum=0; byteNum < columns.length; ++byteNum) {
            int added = unpackByteFromLegacy(columns[byteNum], byteNum, retval);
            assert added >= 0 : String.format("bytes[%d] added %d: %s", byteNum, added, retval);
        }
        return retval;
    }

    /**
     * Packs these columns to the format expected by the legacy system, used by Messages. This is two bytes, with the
     * low-order byte corresponding to column 0, and a 1 or 0 for each column specifying whether that column should
     * be scanned.
     * See the
     * <a href="https://akibaninc.onconfluence.com/display/db/Message+Compendium#MessageCompendium-ScanRowsRequest">
     * message compendium's definition of ScanRowsRequest</a> for more information.
     * @param columns the columns to pack to bytes
     * @return the columns desired
     */
    public static byte[] packToLegacy(Collection<Integer> columns) {
        if (columns.isEmpty()) {
            return new byte[0];
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (int posAbsolute : columns) {
            final int byteNum = posAbsolute / 8;
            final int posRelative = posAbsolute % 8;
            byteBuffer = ensureCapacity(byteBuffer, byteNum + 1);
            final byte activeByteOrig = byteBuffer.get(byteNum);
            final byte activeByteNew = (byte) (activeByteOrig | (1 << posRelative));
            if (activeByteNew != activeByteOrig) {
                byteBuffer.put(byteNum, activeByteNew);
            }
        }
        return byteBuffer.array();
    }

    /**
     * Ensures that the given ByteBuffer is large enough, reallocating it if it isn't.
     *
     * If the given buffer has a capacity &gt;= the required byte count, this method will simply return that
     * instance. Otherwise, it'll allocate a new buffer with the same byte ordering, copy the old one into
     * the new one, and return that. The returned buffer's position is undefined.
     * @param oldBuffer the buffer which may or may not be large enough
     * @param byteCount the required number of bytes
     * @return the given buffer if it was large enough, or a new buffer with the old one copied over
     */
    private static ByteBuffer ensureCapacity(ByteBuffer oldBuffer, int byteCount) {
        if (oldBuffer.capacity() >= byteCount) {
            return oldBuffer;
        }
        final ByteBuffer newBuffer = ByteBuffer.allocate(byteCount);
        newBuffer.order(oldBuffer.order());
        newBuffer.put(oldBuffer.array());
        return newBuffer;
    }
}
