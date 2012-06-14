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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.validation.AISInvariants;

public class TableIndex extends Index
{
    public static TableIndex create(AkibanInformationSchema ais,
                                    Table table,
                                    String indexName,
                                    Integer indexId,
                                    Boolean isUnique,
                                    String constraint)
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
    public void computeFieldAssociations(Map<Table, Integer> ordinalMap)
    {
        freezeColumns();
        AssociationBuilder toIndexRowBuilder = new AssociationBuilder();
        AssociationBuilder toHKeyBuilder = new AssociationBuilder();
        List<Column> indexColumns = new ArrayList<Column>();
        // Add index key fields
        for (IndexColumn iColumn : getKeyColumns()) {
            Column column = iColumn.getColumn();
            indexColumns.add(column);
            toIndexRowBuilder.rowCompEntry(column.getPosition(), -1);
        }
        // Add leafward-biased hkey fields not already included
        int indexColumnPosition = indexColumns.size();
        hKeyColumns = new ArrayList<IndexColumn>();
        HKey hKey = hKey();
        for (HKeySegment hKeySegment : hKey.segments()) {
            Integer ordinal = ordinalMap.get(hKeySegment.table());
            assert ordinal != null : hKeySegment.table();
            toHKeyBuilder.toHKeyEntry(ordinal, -1);
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column column = hKeyColumn.column();
                if (!indexColumns.contains(column)) {
                    if (table.getColumnsIncludingInternal().contains(column)) {
                        toIndexRowBuilder.rowCompEntry(column.getPosition(), -1);
                    } else {
                        assert hKeySegment.table().isUserTable() : this;
                        toIndexRowBuilder.rowCompEntry(-1, hKeyColumn.positionInHKey());
                    }
                    indexColumns.add(column);
                    hKeyColumns.add(new IndexColumn(this, column, indexColumnPosition, true, 0));
                    indexColumnPosition++;
                }
                toHKeyBuilder.toHKeyEntry(-1, indexColumns.indexOf(column));
            }
        }
        allColumns = new ArrayList<IndexColumn>();
        allColumns.addAll(keyColumns);
        allColumns.addAll(hKeyColumns);
        indexRowComposition = toIndexRowBuilder.createIndexRowComposition();
        indexToHKey = toHKeyBuilder.createIndexToHKey();
    }

    @Override
    public Table leafMostTable() {
        return table;
    }

    @Override
    public Table rootMostTable() {
        return table;
    }

    @Override
    public void checkMutability() {
        table.checkMutability();
    }

    public Table getTable()
    {
        return table;
    }

    public IndexToHKey indexToHKey()
    {
        return indexToHKey;
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
    private List<IndexColumn> hKeyColumns;
    private IndexToHKey indexToHKey;
}
