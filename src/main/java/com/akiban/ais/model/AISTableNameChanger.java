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

package com.akiban.ais.model;

public class AISTableNameChanger {
    public AISTableNameChanger(UserTable table)
    {
        this.table = table;
        this.newSchemaName = table.getName().getSchemaName();
        this.newTableName = table.getName().getTableName();
    }

    public AISTableNameChanger(UserTable table, String newSchemaName, String newTableName) {
        this.table = table;
        this.newSchemaName = newSchemaName;
        this.newTableName = newTableName;
    }

    public void setSchemaName(String newSchemaName) {
        this.newSchemaName = newSchemaName;
    }

    public void setNewTableName(String newTableName) {
        this.newTableName = newTableName;
    }

    public void doChange() {
        table.getAIS().removeTable(table.getName());
        TableName newName = new TableName(newSchemaName, newTableName);

        // Index is the only object that refers to a table by name (in its own name)
        for(Index index : table.getIndexesIncludingInternal()) {
            index.setIndexName(new IndexName(newName, index.getIndexName().getName()));
        }

        table.setTableName(newName);
        table.getAIS().addUserTable(table);
    }


    UserTable table;
    String newSchemaName;
    String newTableName;
}
