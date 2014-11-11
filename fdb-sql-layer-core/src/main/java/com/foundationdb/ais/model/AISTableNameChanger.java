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

package com.foundationdb.ais.model;

import java.util.ArrayList;

public class AISTableNameChanger {
    public AISTableNameChanger(Table table)
    {
        this.table = table;
        this.newSchemaName = table.getName().getSchemaName();
        this.newTableName = table.getName().getTableName();
    }

    public AISTableNameChanger(Table table, TableName newName) {
        this(table, newName.getSchemaName(), newName.getTableName());
    }

    public AISTableNameChanger(Table table, String newSchemaName, String newTableName) {
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
        AkibanInformationSchema ais = table.getAIS();
        ais.removeTable(table.getName());
        TableName newName = new TableName(newSchemaName, newTableName);

        // Fix indexes because index names incorporate table name
        for (Index index : table.getIndexesIncludingInternal()) {
            index.setIndexName(new IndexName(newName, index.getIndexName().getName()));
        }
        // Join names too. Copy the joins because ais.getJoins() will be updated inside the loop
        NameGenerator nameGenerator = new DefaultNameGenerator();
        for (Join join : new ArrayList<>(ais.getJoins().values())) {
            if (join.getParent().getName().equals(table.getName())) {
                String newJoinName = nameGenerator.generateJoinName(newName,
                                                                    join.getChild().getName(),
                                                                    join.getJoinColumns());
                join.replaceName(newJoinName);
            } else if (join.getChild().getName().equals(table.getName())) {
                String newJoinName = nameGenerator.generateJoinName(join.getParent().getName(),
                                                                    newName,
                                                                    join.getJoinColumns());
                join.replaceName(newJoinName);
            }
        }
        // Rename the table and put back in AIS
        table.setTableName(newName);
        ais.addTable(table);
    }


    Table table;
    String newSchemaName;
    String newTableName;
}
