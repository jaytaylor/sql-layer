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