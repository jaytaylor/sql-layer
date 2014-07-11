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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableIndex extends Index
{
    public static TableIndex create(AkibanInformationSchema ais,
                                    Table table,
                                    String indexName,
                                    Integer indexId,
                                    Boolean isUnique,
                                    Boolean isPrimary) {
        return create(ais, table, indexName, indexId, isUnique, isPrimary, null);
    }

    public static TableIndex create(AkibanInformationSchema ais,
                                    Table table,
                                    String indexName,
                                    Integer indexId,
                                    Boolean isUnique,
                                    Boolean isPrimary,
                                    TableName constraintName)
    {
        table.checkMutability();
        ais.checkMutability();
        AISInvariants.checkDuplicateConstraintsInSchema(ais, constraintName);
        AISInvariants.checkDuplicateIndexesInTable(table, indexName);
        TableIndex index = new TableIndex(table, indexName, indexId, isUnique, isPrimary, constraintName);
        table.addIndex(index);
        if(constraintName != null) {
            ais.addConstraint(index);
        }
        return index;
    }

    /**
     * Create an independent copy of an existing TableIndex.
     * @param table Destination Table.
     * @param index TableIndex to to copy.
     * @return The new copy of the TableIndex.
     */
    public static TableIndex create(Table table, TableIndex index)
    {
        TableIndex copy = create(table.getAIS(), table, index.getIndexName().getName(), index.getIndexId(),
                                  index.isUnique(),
                                  index.isPrimaryKey(), index.getConstraintName());
        if (index.getIndexMethod() == IndexMethod.Z_ORDER_LAT_LON) {
            copy.markSpatial(index.firstSpatialArgument(), index.dimensions());
        }
        return copy;
    }

    public TableIndex(Table table, String indexName, Integer indexId, Boolean isUnique, Boolean isPrimary, TableName constraintName)
    {
        // Index check indexName for null state.
        super(table.getName(), indexName, indexId, isUnique, isPrimary, constraintName);
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
        List<Column> indexColumns = new ArrayList<>();
        // Add index key fields
        for (IndexColumn iColumn : getKeyColumns()) {
            Column column = iColumn.getColumn();
            indexColumns.add(column);
            toIndexRowBuilder.rowCompEntry(column.getPosition(), -1);
        }
        // Add leafward-biased hkey fields not already included
        int indexColumnPosition = indexColumns.size();
        List<IndexColumn> hKeyColumns = new ArrayList<>();
        HKey hKey = hKey();
        for (HKeySegment hKeySegment : hKey.segments()) {
            // TODO: ordinalMap is null if this function is called while marking an index as spatial.
            // TODO: This is temporary, working on spatial indexes before DDL support. Once the parser
            // TODO: supports spatial indexes, they'll be born as spatial, and we won't have to recompute
            // TODO: field associations after the fact.
            // By the way, it might actually safe to remove the reliance on ordinalMap and always get
            // the ordinal from the rowdef.
            Integer ordinal =
                ordinalMap == null
                ? hKeySegment.table().getOrdinal()
                : ordinalMap.get(hKeySegment.table());
            assert ordinal != null : hKeySegment.table();
            toHKeyBuilder.toHKeyEntry(ordinal, -1);
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column column = hKeyColumn.column();
                if (!indexColumns.contains(column)) {
                    if (table.getColumnsIncludingInternal().contains(column)) {
                        toIndexRowBuilder.rowCompEntry(column.getPosition(), -1);
                    } else {
                        toIndexRowBuilder.rowCompEntry(-1, hKeyColumn.positionInHKey());
                    }
                    indexColumns.add(column);
                    hKeyColumns.add(new IndexColumn(this, column, indexColumnPosition, true, 0));
                    indexColumnPosition++;
                }
                toHKeyBuilder.toHKeyEntry(-1, indexColumns.indexOf(column));
            }
        }
        allColumns = new ArrayList<>();
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

    @Override
    public Collection<Integer> getAllTableIDs() {
        return Collections.singleton(table.getTableId());
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
            hKey = table.hKey();
        }
        return hKey;
    }

    private final Table table;
    private HKey hKey;
    private IndexToHKey indexToHKey;
}
