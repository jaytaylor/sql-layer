package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class ColumnSet {

    public static int unpackByteFromLegacy(byte theByte, int byteNum, Set<ColumnId> out) {
        int added = 0;
        for(int relativePos=0; relativePos < 8; ++relativePos) {
            if ( 0!= (theByte & (1 << relativePos))) {
                if (out.add( new ColumnId( (byteNum*8) + relativePos) )) {
                    ++added;
                }
            }
        }
        return added;
    }

    public static Set<ColumnId> unpackFromLegacy(byte[] columns) {
        ArgumentValidation.notNull("columns", columns);
        if (columns.length == 0) {
            return Collections.emptySet();
        }
        Set<ColumnId> retval = new HashSet<ColumnId>();
        for (int byteNum=0; byteNum < columns.length; ++byteNum) {
            if (columns[byteNum] > 0) {
                int added = unpackByteFromLegacy(columns[byteNum], byteNum, retval);
                assert added > 0 : String.format("bytes[%d] added %d: %s", byteNum, added, retval);
            }
        }
        return retval;
    }

    /**
     * Packs these columns to the format expected by the legacy system, used by Messages. This is two bytes, with the
     * low-order byte corresponding to column 0, and a 1 or 0 for each column specifying whether that column should
     * be scanned.
     * See the
     * <a href="https://akibainc.onconfluence.com/display/db/Message+Compendium#MessageCompendium-ScanRowsRequest">
     * message compendium's definition of ScanRowsRequest</a> for more information.
     * @return the columns desired
     */
    public static byte[] packToLegacy(Collection<ColumnId> columns, IdResolver resolver) {
        if (columns.isEmpty()) {
            return new byte[0];
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (ColumnId column : columns) {
            final int posAbsolute = column.getPosition();
            final int byteNum = ((posAbsolute + 8) / 8) - 1;
            final int posRelative = posAbsolute - byteNum*8;
            assert (posRelative <= 7) && (posRelative >=0)
                    : String.format("0x%X had bit %d in byte %d", posAbsolute, byteNum, posRelative);
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
        newBuffer.order( oldBuffer.order() );
        newBuffer.put(oldBuffer);
        return newBuffer;
    }
}
