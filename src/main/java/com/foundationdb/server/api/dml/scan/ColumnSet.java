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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.foundationdb.util.ArgumentValidation;

public final class ColumnSet {

    public static Set<Integer> ofPositions(int... positions) {
        Set<Integer> asSet = new HashSet<>();
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
        Set<Integer> retval = new HashSet<>();
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
