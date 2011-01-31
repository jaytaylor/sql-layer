/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

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
