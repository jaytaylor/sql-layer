/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
