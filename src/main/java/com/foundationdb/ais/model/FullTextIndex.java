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

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.error.BranchingGroupIndexException;
import com.foundationdb.server.error.IndexColNotInGroupException;

import java.util.*;

public class FullTextIndex extends Index
{
    /* Index */

    @Override
    public HKey hKey() {
        return indexedTable.hKey();
    }

    @Override
    public boolean isTableIndex() {
        return false;
    }

    @Override
    public boolean isGroupIndex() {
        return false;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.FULL_TEXT;
    }

    @Override
    public IndexMethod getIndexMethod() {
        return IndexMethod.FULL_TEXT;
    }

    @Override
    public void computeFieldAssociations(Map<Table,Integer> ordinalMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table leafMostTable() {
        // This is not entirely well-defined, since more than one
        // descendant to the same depth can be indexed.
        Table deepest = null;
        for (IndexColumn indexColumn : keyColumns) {
            if ((deepest == null) || 
                (indexColumn.getColumn().getTable().getDepth() > deepest.getDepth())) {
                deepest = indexColumn.getColumn().getTable();
            }
        }
        return deepest;
    }

    @Override
    public Table rootMostTable() {
        Table shallowest = null;
        for (IndexColumn indexColumn : keyColumns) {
            if ((shallowest == null) || 
                (indexColumn.getColumn().getTable().getDepth() < shallowest.getDepth())) {
                shallowest = indexColumn.getColumn().getTable();
            }
        }
        return shallowest;
    }

    @Override
    public void checkMutability() {
        indexedTable.checkMutability();
    }

    @Override
    public Collection<Integer> getAllTableIDs() {
        Set<Integer> ids = new HashSet<>();
        for (IndexColumn indexColumn : keyColumns) {
            ids.add(indexColumn.getColumn().getTable().getTableId());
        }
        ids.add(indexedTable.getTableId());
        return ids;
    }

    @Override
    public void addColumn(IndexColumn indexColumn) {
        Table table = indexColumn.getColumn().getTable();
        if (!((table == indexedTable) ||
              table.isDescendantOf(indexedTable) ||
              indexedTable.isDescendantOf(table))) {
            if (table.getGroup() != indexedTable.getGroup()) {
                throw new IndexColNotInGroupException(indexColumn.getIndex().getIndexName().getName(),
                                                      indexColumn.getColumn().getName());
            }
            else {
                throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                       table.getName(),
                                                       indexedTable.getName());
            }
        }
        super.addColumn(indexColumn);
        table.addFullTextIndex(this);
    }

    /* FullTextIndex */

    public Table getIndexedTable() {
        return indexedTable;
    }

    public static FullTextIndex create(AkibanInformationSchema ais,
                                       Table table, String indexName,
                                       Integer indexId) {
        return create(ais, table, indexName, indexId, null);
    }

    public static FullTextIndex create(AkibanInformationSchema ais,
                                       Table table, String indexName, 
                                       Integer indexId, TableName constraintName)
    {
        ais.checkMutability();
        if(constraintName != null) {
            throw new IllegalArgumentException("Full Text indexes are never constraints");
        }
        table.checkMutability();
        AISInvariants.checkDuplicateIndexesInTable(table, indexName);
        FullTextIndex index = new FullTextIndex(table, indexName, indexId);
        table.addFullTextIndex(index);
        return index;
    }

    private FullTextIndex(Table indexedTable, String indexName, Integer indexId)
    {
        super(indexedTable.getName(), indexName, indexId, false, false, null, null);
        this.indexedTable = indexedTable;
    }
    
    private final Table indexedTable;
}
