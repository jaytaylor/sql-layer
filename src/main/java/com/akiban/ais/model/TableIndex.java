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

import java.util.Map;

import com.akiban.ais.model.validation.AISInvariants;

public class TableIndex extends Index
{
    public static TableIndex create(AkibanInformationSchema ais, Table table, String indexName, Integer indexId,
                                    Boolean isUnique, String constraint)
    {
        table.checkMutability();
        ais.checkMutability();
        AISInvariants.checkDuplicateIndexesInTable(table, indexName);
        TableIndex index = new TableIndex(table, indexName, indexId, isUnique, constraint);
        table.addIndex(index);
        return index;
    }

    public TableIndex(Table table, String indexName, Integer indexId, Boolean isUnique, String constraint)
    {
        // Index check indexName for null state.
        super(table.getName(), indexName, indexId, isUnique, constraint);
        this.table = table;
    }

    @Override
    public boolean isTableIndex()
    {
        return true;
    }

    @Override
    public void computeFieldAssociations(Map<Table, Integer> ordinalMap) {
        computeFieldAssociations(ordinalMap, getTable(), null);
    }

    @Override
    protected Column indexRowCompositionColumn(HKeyColumn hKeyColumn) {
        return hKeyColumn.column();
    }

    @Override
    public Table leafMostTable() {
        return getTable();
    }

    @Override
    public Table rootMostTable() {
        return getTable();
    }

    @Override
    public void checkMutability() {
        table.checkMutability();
    }

    public Table getTable()
    {
        return table;
    }

    // For a user table index: the user table hkey
    // For a group table index: the hkey of the leafmost user table, but with user table columns replaced by
    // group table columns.
    @Override
    public HKey hKey()
    {
        if (hKey == null) {
            if (table.isUserTable()) {
                hKey = ((UserTable) table).hKey();
            } else {
                // Find the user table corresponding to this index. Currently, the columns of a group table index all
                // correspond to the same user table.
                UserTable userTable = null;
                for (IndexColumn indexColumn : getKeyColumns()) {
                    Column userColumn = indexColumn.getColumn().getUserColumn();
                    if (userTable == null) {
                        userTable = (UserTable) userColumn.getTable();
                    } else {
                        assert userTable == userColumn.getTable();
                    }
                }
                // Construct an hkey like userTable.hKey(), but with group columns replacing user columns.
                assert userTable != null : this;
                hKey = userTable.branchHKey();
            }
        }
        return hKey;
    }

    private final Table table;
    private HKey hKey;
}
