package com.akiban.cserver.api.common;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.api.dml.NoSuchTableException;
import com.akiban.util.CacheMap;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>A wrapper for a table ID, identified either by the table's int ID or by the table's name.</p>
 *
 * <p>A TableId is a union of TableName and int ID; at least one of those must be defined for each TableId.
 * If both are known, the int always takes precedence. A TableId which has an int defined is considered
 * <strong>resolved</strong>, and a TableId which does not is known as <strong>unresolved</strong>. Once a TableID
 * has an int ID defined, that ID cannot be changed. Only resolved TableId instances can be used in a Set or
 * as the keys to a Map (see below).</p>
 *
 * <p>A TableId represents a table in the abstract, but there is no guarantee that the table represented actually
 * exists in the system; this is true whether the TableId is resolved.</p>
 *
 * <p>This class' equality and hash code depend on the int ID of the table it represents. This may not be known
 * at time of construction, if the TableId hasn't been resolved. As such, an unresolved TableId has an undefined
 * equality and hash code, and cannot be used in any contexts (such as a Set or a Map's key) which require
 * them. Any attempt to do so will result in a ResolutionException being thrown.</p>
 */
public final class TableId extends ByteBufferWriter {

    private final static CacheMap<Integer,TableId> cache = new CacheMap<Integer, TableId>(new CacheMap.Allocator<Integer,TableId>() {
        @Override
        public TableId allocateFor(Integer key) {
            return new TableId(key);
        }
    });
    
    private final AtomicReference<Integer> tableId = new AtomicReference<Integer>();
    private final TableName tableName;

    public static TableId of(int tableId) {
        return cache.get(tableId);
    }

    private TableId(int tableId) {
        this.tableId.set(tableId);
        this.tableName = null;
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public TableId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, 4);
        tableId.set(readFrom.getInt());
        this.tableName = null;
    }

    public TableId(String schemaName, String tableName) {
        this.tableName = new TableName(schemaName, tableName);
    }

    private int getIdOrThrow() throws ResolutionException {
        Integer tableIdInt = tableId.get();
        if (tableIdInt == null) {
            throw new ResolutionException(this);
        }
        return tableIdInt;
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(getIdOrThrow());
    }

    public boolean isResolved() {
        return tableId.get() != null;
    }

    public int getTableId(IdResolver resolver) throws NoSuchTableException, ResolutionException {
        Integer tableIdInteger = tableId.get();
        if (tableIdInteger != null) {
            return tableIdInteger;
        }

        int tableIdInt = resolver.tableId(tableName);
        if (!tableId.compareAndSet(null, tableIdInt)) {
            Integer old = tableId.get();
            if (old == null || (old != tableIdInt) ) {
                throw new ResolutionException(this);
            }
        }
        return tableIdInt;
    }

    public TableName getTableName(IdResolver resolver) throws NoSuchTableException {
        if (tableName != null) {
            return tableName;
        }
        return resolver.tableName(tableId.get());
    }

    @Override
    public boolean equals(Object o) {
        Integer myIdInt = tableId.get();
        if (myIdInt == null) {
            throw new ResolutionException();
        }

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableId tableId1 = (TableId) o;
        Integer otherIdInt = tableId1.tableId.get();
        if (otherIdInt == null) {
            throw new ResolutionException();
        }
        return myIdInt.equals(otherIdInt);
    }

    @Override
    public int hashCode() {
        Integer tableIdInt = tableId.get();
        if (tableIdInt == null) {
            throw new ResolutionException();
        }
        return tableIdInt.hashCode();
    }
}