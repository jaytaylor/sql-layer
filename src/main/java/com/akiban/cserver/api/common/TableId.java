package com.akiban.cserver.api.common;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.api.dml.NoSuchTableException;
import com.akiban.cserver.manage.SchemaManager;

import java.nio.ByteBuffer;

public final class TableId extends ByteBufferWriter {
    private final int NO_TABLE_ID = Integer.MIN_VALUE;
    private final int tableId;
    private final TableName tableName;

    public TableId(int tableId) {
        this.tableId = tableId;
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
        tableId = readFrom.getInt();
        this.tableName = null;
    }

    public TableId(String schemaName, String tableName) {
        this.tableId = NO_TABLE_ID;
        this.tableName = new TableName(schemaName, tableName);
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(tableId);
    }

    public int getTableId(IdResolver schemaManager) throws NoSuchTableException {
        if (tableName != null) {
            return schemaManager.tableId(tableName);
        }
        return tableId;
    }

    public TableName getTableName(IdResolver resolver) throws NoSuchTableException {
        if (tableName != null) {
            return tableName;
        }
        return resolver.tableName(tableId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableId tableId1 = (TableId) o;

        return tableId == tableId1.tableId;

    }

    @Override
    public int hashCode() {
        return tableId;
    }
}