
package com.akiban.ais.model;

import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.IndexColNotInGroupException;

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
        UserTable deepest = null;
        for (IndexColumn indexColumn : keyColumns) {
            if ((deepest == null) || 
                (indexColumn.getColumn().getUserTable().getDepth() > deepest.getDepth())) {
                deepest = indexColumn.getColumn().getUserTable();
            }
        }
        return deepest;
    }

    @Override
    public Table rootMostTable() {
        UserTable shallowest = null;
        for (IndexColumn indexColumn : keyColumns) {
            if ((shallowest == null) || 
                (indexColumn.getColumn().getUserTable().getDepth() < shallowest.getDepth())) {
                shallowest = indexColumn.getColumn().getUserTable();
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
        return ids;
    }

    @Override
    public void addColumn(IndexColumn indexColumn) {
        UserTable table = indexColumn.getColumn().getUserTable();
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

    public UserTable getIndexedTable() {
        return indexedTable;
    }

    public static FullTextIndex create(AkibanInformationSchema ais,
                                       UserTable table, String indexName, 
                                       Integer indexId)
    {
        ais.checkMutability();
        table.checkMutability();
        AISInvariants.checkDuplicateIndexesInTable(table, indexName);
        FullTextIndex index = new FullTextIndex(table, indexName, indexId);
        table.addFullTextIndex(index);
        return index;
    }

    public static final String FULL_TEXT_CONSTRAINT = "FULL_TEXT";

    public FullTextIndex(UserTable indexedTable, String indexName, Integer indexId)
    {
        super(indexedTable.getName(), indexName, indexId, false, FULL_TEXT_CONSTRAINT);
        this.indexedTable = indexedTable;
    }
    
    private final UserTable indexedTable;
}
