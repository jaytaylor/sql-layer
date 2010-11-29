package com.akiban.cserver.store;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.model.TableName;

public class CreateTableResult {
    private boolean success = false;
    private int tableId = -1;
    private TableName tableName = null;
    private boolean hasAutoInc = false;
    private long autoIncValue = -1L;

    void fillInInfo(SchemaDef.UserTableDef tableDef, int tableId) {
        this.tableId = tableId;
        this.tableName = new TableName(tableDef.getCName().getSchema(), tableDef.getCName().getName());
        SchemaDef.ColumnDef autoIncColumn = tableDef.getAutoIncrementColumn();
        if (autoIncColumn != null) {
            this.hasAutoInc = true;
            this.autoIncValue = autoIncColumn.defaultAutoIncrement();
        }
        success = true;
    }

    public Integer getTableId() {
        return tableId;
    }

    public TableName getTableName() {
        return tableName;
    }

    public boolean wasSuccessful() {
        return success;
    }

    @Override
    public String toString() {
        return "CreateTableResult[" +
                "tableId=" + tableId +
                ", tableName=" + tableName +
                ", success=" + success +
                ']';
    }

    public boolean autoIncrementDefined() {
        return hasAutoInc;
    }

    public long defaultAutoIncrement() {
        return autoIncValue;
    }
}
