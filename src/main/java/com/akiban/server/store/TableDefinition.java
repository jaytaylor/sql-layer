
package com.akiban.server.store;

import com.akiban.ais.model.TableName;

public class TableDefinition {
    private final int tableId;
    private final String schemaName;
    private final String tableName;
    private final String ddl;

    public TableDefinition(int tableId, String schemaName, String tableName, String ddl) {
        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.ddl = ddl;
    }

    public TableDefinition(int tableId, TableName name, String ddl) {
        this(tableId, name.getSchemaName(), name.getTableName(), ddl);
    }

    public String getDDL() {
        return ddl;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "TableDefinition[" + tableId + ": "
                + TableName.create(schemaName, tableName) + ']';
    }
}