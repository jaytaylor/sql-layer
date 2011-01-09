package com.akiban.cserver.store;

import com.akiban.ais.model.TableName;

public class TableDefinition {
    private int tableId;
    private final String schemaName;
    private final String tableName;
    private final String ddl;

    public TableDefinition(int tableId, String schemaName,
            String tableName, String ddl) {
        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.ddl = ddl;
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
    
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    @Override
    public String toString() {
        return "TableDefinition[" + tableId + ": "
                + TableName.create(schemaName, tableName) + ']';
    }
}