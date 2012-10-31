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
import com.akiban.server.geophile.Space;
import com.akiban.server.geophile.SpaceLatLon;

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
                                    String constraint)
    {
        table.checkMutability();
        ais.checkMutability();
        AISInvariants.checkDuplicateIndexesInTable(table, indexName);
        TableIndex index = new TableIndex(table, indexName, indexId, isUnique, constraint);
        table.addIndex(index);
        return index;
    }

    public String toString()
    {
        return
            isSpatial()
            ? super.toString() + space.toString()
            : super.toString();
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
                                  index.getConstraint());
        if (index.getIndexMethod() == IndexMethod.Z_ORDER_LAT_LON) {
            copy.markSpatial(index.firstSpatialArgument(), index.dimensions());
        }
        return copy;
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
            // TODO: ordinalMap is null if this function is called while marking an index as spatial.
            // TODO: This is temporary, working on spatial indexes before DDL support. Once the parser
            // TODO: supports spatial indexes, they'll be born as spatial, and we won't have to recompute
            // TODO: field associations after the fact.
            // By the way, it might actually safe to remove the reliance on ordinalMap and always get
            // the ordinal from the rowdef.
            Integer ordinal =
                ordinalMap == null
                ? hKeySegment.table().rowDef().getOrdinal()
                : ordinalMap.get(hKeySegment.table());
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
        uniqueAndMayContainNulls = false;
        if (!isPrimaryKey() && isUnique()) {
            for (IndexColumn indexColumn : getKeyColumns()) {
                if (indexColumn.getColumn().getNullable()) {
                    uniqueAndMayContainNulls = true;
                }
            }
        }
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

    @Override
    public boolean isUniqueAndMayContainNulls()
    {
        return uniqueAndMayContainNulls;
    }

    @Override
    public IndexMethod getIndexMethod()
    {
        if (space != null)
            return IndexMethod.Z_ORDER_LAT_LON;
        else
            return IndexMethod.NORMAL;
    }

    public void markSpatial(int firstSpatialArgument, int dimensions)
    {
        checkMutability();
        if (dimensions != Space.LAT_LON_DIMENSIONS) {
            // Only lat/lon for now
            throw new IllegalArgumentException();
        }
        this.firstSpatialArgument = firstSpatialArgument;
        this.space = SpaceLatLon.create();
    }

    public int firstSpatialArgument()
    {
        return firstSpatialArgument;
    }

    public int dimensions()
    {
        // Only lat/lon for now
        return Space.LAT_LON_DIMENSIONS;
    }

    public Space space()
    {
        return space;
    }

    // For a user table index: the user table hkey
    // For a group table index: the hkey of the leafmost user table, but with user table columns replaced by
    // group table columns.
    @Override
    public HKey hKey()
    {
        if (hKey == null) {
            hKey = ((UserTable) table).hKey();
        }
        return hKey;
    }

    private final Table table;
    private HKey hKey;
    private List<IndexColumn> hKeyColumns;
    private IndexToHKey indexToHKey;
    private boolean uniqueAndMayContainNulls;
    // For a spatial index
    private Space space;
    private int firstSpatialArgument;

}
