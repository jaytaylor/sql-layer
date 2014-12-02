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
import com.foundationdb.qp.storeadapter.SpatialHelper;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;

import java.util.*;

public abstract class Index extends HasStorage implements Visitable, Constraint
{
    public abstract HKey hKey();
    public abstract boolean isTableIndex();
    public abstract void computeFieldAssociations(Map<Table,Integer> ordinalMap);
    public abstract Table leafMostTable();
    public abstract Table rootMostTable();
    public abstract void checkMutability();
    public abstract Collection<Integer> getAllTableIDs();

    protected Index(TableName tableName,
                    String indexName,
                    Integer indexId,
                    Boolean isUnique,
                    Boolean isPrimary,
                    TableName constraintName,
                    JoinType joinType)
    {
        AISInvariants.checkNullName(indexName, "index", "index name");

        if(((isUnique == null) || !isUnique) && (constraintName != null)) {
            throw new IllegalStateException("Unexpected constraint name for non-unique index: " + indexName);
        }

        this.indexName = new IndexName(tableName, indexName);
        this.indexId = indexId;
        this.isUnique = isUnique;
        this.isPrimary = isPrimary;
        this.joinType = joinType;
        this.constraintName = constraintName;
        keyColumns = new ArrayList<>();
    }

    public boolean isGroupIndex()
    {
        return !isTableIndex();
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getTypeString());
        buffer.append("(");
        buffer.append(getNameString());
        buffer.append(keyColumns.toString());
        buffer.append(")");
        if (space != null) {
            buffer.append(space.toString());
        }
        return buffer.toString();
    }

    void addColumn(IndexColumn indexColumn)
    {
        if (columnsFrozen) {
            throw new IllegalStateException("can't add column because columns list is frozen");
        }
        keyColumns.add(indexColumn);
        columnsStale = true;
    }

    public void freezeColumns() {
        if (!columnsFrozen) {
            sortColumnsIfNeeded();
            columnsFrozen = true;
        }
    }

    public boolean isUnique()
    {
        return isUnique;
    }

    public boolean isPrimaryKey()
    {
        return isPrimary;
    }

    public boolean isConnectedToFK() {
        Schema schema = getAIS().getSchema(this.getSchemaName());
        if (schema.hasConstraint(indexName.getName()) && (schema.getConstraint(indexName.getName()) instanceof ForeignKey)) {
            return true;
        }
        return false;
    }

    public IndexName getIndexName()
    {
        return indexName;
    }

    public void setIndexName(IndexName name)
    {
        indexName = name;
    }

    /**
     * Return columns declared as part of the index definition.
     * @return list of columns
     */
    public List<IndexColumn> getKeyColumns()
    {
        sortColumnsIfNeeded();
        return keyColumns;
    }

    /**
     * Return all columns that make up the physical index key. This includes declared columns and hkey columns.
     * @return list of columns
     */
    public List<IndexColumn> getAllColumns() {
        return allColumns;
    }

    public IndexMethod getIndexMethod()
    {
        if (space != null)
            return IndexMethod.Z_ORDER_LAT_LON;
        else
            return IndexMethod.NORMAL;
    }

    public void markSpatial(int firstSpatialArgument, int spatialColumns)
    {
        checkMutability();
        if (spatialColumns != Spatial.LAT_LON_DIMENSIONS && spatialColumns != 1) {
            // Either 1 or 2 is acceptable for now. 1: A blob containing a serialized spatial object. 2: lat/lon
            throw new IllegalArgumentException();
        }
        this.firstSpatialArgument = firstSpatialArgument;
        this.lastSpatialArgument = firstSpatialArgument + spatialColumns - 1;
        this.space = Spatial.createLatLonSpace();
    }

    public int firstSpatialArgument()
    {
        return firstSpatialArgument;
    }

    public int lastSpatialArgument()
    {
        return lastSpatialArgument;
    }

    public int dimensions()
    {
        // Only lat/lon for now
        return Spatial.LAT_LON_DIMENSIONS;
    }

    public int spatialColumns()
    {
        // For lat/lon, this computes 2. For blobs storing spatial objects, 1.
        // Non-spatial: 0
        return isSpatial() ? lastSpatialArgument - firstSpatialArgument + 1 : 0;
    }

    public Space space()
    {
        return space;
    }

    public final boolean isSpatial()
    {
        switch (getIndexMethod()) {
        case Z_ORDER_LAT_LON:
            return true;
        default:
            return false;
        }
    }

    private void sortColumnsIfNeeded() {
        if (columnsStale) {
            Collections.sort(keyColumns,
                    new Comparator<IndexColumn>() {
                        @Override
                        public int compare(IndexColumn x, IndexColumn y) {
                            return x.getPosition() - y.getPosition();
                        }
                    });
            columnsStale = false;
        }
    }

    public Integer getIndexId()
    {
        return indexId;
    }

    public void setIndexId(Integer indexId)
    {
        this.indexId = indexId;
    }

    public IndexType getIndexType()
    {
        return isTableIndex() ? IndexType.TABLE : IndexType.GROUP;
    }

    public IndexRowComposition indexRowComposition()
    {
        return indexRowComposition;
    }

    protected static class AssociationBuilder {
        /**
         * @param fieldPosition entry of {@link IndexRowComposition#fieldPositions}
         * @param hkeyPosition entry of {@link IndexRowComposition#hkeyPositions}
         */
        void rowCompEntry(int fieldPosition, int hkeyPosition) {
            list1.add(fieldPosition); list2.add(hkeyPosition);
        }

        /**
         * @param ordinal entry of {@link IndexToHKey#ordinals}
         * @param indexRowPosition entry of {@link IndexToHKey#indexRowPositions}
         */
        void toHKeyEntry(int ordinal, int indexRowPosition) {
            list1.add(ordinal); list2.add(indexRowPosition);
        }

        IndexRowComposition createIndexRowComposition() {
            return new IndexRowComposition(asArray(list1), asArray(list2));
        }

        IndexToHKey createIndexToHKey() {
            return new IndexToHKey(asArray(list1), asArray(list2));
        }

        private int[] asArray(List<Integer> list) {
            int[] array = new int[list.size()];
            for(int i = 0; i < list.size(); ++i) {
                array[i] = list.get(i);
            }
            return array;
        }

        private List<Integer> list1 = new ArrayList<>();
        private List<Integer> list2 = new ArrayList<>();
    }

    public boolean containsTableColumn(TableName tableName, String columnName) {
        for(IndexColumn iCol : keyColumns) {
            Column column = iCol.getColumn();
            if(column.getTable().getName().equals(tableName) && column.getName().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    // Visitable

    /** Visit this instance and then all index columns. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
        // Not present until computeFieldAssociations is called
        List<IndexColumn> cols = (allColumns == null) ? keyColumns : allColumns;
        for(IndexColumn ic : cols) {
            ic.visit(visitor);
        }
    }

    // akCollators and types provide type info for physical index rows.
    // Physical != logical for spatial indexes.

    public TInstance[] types()
    {
        ensureTypeInfo();
        return types;
    }

    private void ensureTypeInfo()
    {
        if (types == null) {
            synchronized (this) {
                if (types == null) {
                    int physicalColumns;
                    int firstSpatialColumn;
                    if (isSpatial()) {
                        physicalColumns = allColumns.size() - spatialColumns() + 1;
                        firstSpatialColumn = firstSpatialArgument();
                    } else {
                        physicalColumns = allColumns.size();
                        firstSpatialColumn = Integer.MAX_VALUE;
                    }
                    TInstance[] localTInstances = null;
                    localTInstances = new TInstance[physicalColumns];
                    int logicalColumn = 0;
                    int physicalColumn = 0;
                    int nColumns = allColumns.size();
                    while (logicalColumn < nColumns) {
                        if (logicalColumn == firstSpatialColumn) {
                            localTInstances[physicalColumn] =
                                MNumeric.BIGINT.instance(SpatialHelper.isNullable(this));
                            logicalColumn += spatialColumns();
                        } else {
                            IndexColumn indexColumn = allColumns.get(logicalColumn);
                            Column column = indexColumn.getColumn();
                            localTInstances[physicalColumn] = column.getType();
                            logicalColumn++;
                        }
                        physicalColumn++;
                    }
                    types = localTInstances;
                }
            }
        }
     }

    public static boolean isSpatialCompatible(Index index)
    {
        boolean isSpatialCompatible = false;
        if (index.isSpatial()) {
            if (index.firstSpatialArgument() == index.lastSpatialArgument()) {
                // Serialized spatial object
                isSpatialCompatible = isTextOrBinary(index.getKeyColumns().get(index.firstSpatialArgument()).getColumn());
            } else {
                // Lat/Lon
                isSpatialCompatible = true;
                for (int d = index.firstSpatialArgument(); d <= index.lastSpatialArgument(); d++) {
                    isSpatialCompatible =
                        isSpatialCompatible &&
                        isFixedDecimal(index.getKeyColumns().get(d).getColumn());
                }
            }
        }
        return isSpatialCompatible;
    }

    private static boolean isFixedDecimal(Column column) {
        return column.getType().typeClass() instanceof TBigDecimal;
    }

    private static boolean isTextOrBinary(Column column) {
        TClass columnType = column.getType().typeClass();
        // TBD: Is this right? What types can store serialized spatial objects?
        return
            columnType instanceof TBinary ||
            columnType instanceof TString;
    }

    public static final String PRIMARY = "PRIMARY";
    private final Boolean isUnique;
    private final Boolean isPrimary;
    private final JoinType joinType;
    private Integer indexId;
    private IndexName indexName;
    private boolean columnsStale = true;
    private boolean columnsFrozen = false;
    protected IndexRowComposition indexRowComposition;
    protected List<IndexColumn> keyColumns;
    protected List<IndexColumn> allColumns;
    private volatile TInstance[] types;
    private TableName constraintName;
    // For a spatial index
    private Space space;
    private int firstSpatialArgument;
    private int lastSpatialArgument;

    public enum JoinType {
        LEFT, RIGHT
    }

    public static enum IndexType {
        TABLE("TABLE"),
        GROUP("GROUP"),
        FULL_TEXT("FULL_TEXT")
        ;

        private IndexType(String asString) {
            this.asString = asString;
        }

        @Override
        public final String toString() {
            return asString;
        }

        private final String asString;
    }

    public enum IndexMethod {
        NORMAL, Z_ORDER_LAT_LON, FULL_TEXT
    }

    // HasStorage

    @Override
    public AkibanInformationSchema getAIS() {
        return leafMostTable().getAIS();
    }

    @Override
    public String getTypeString() {
        return "Index";
    }

    @Override
    public String getNameString() {
        return indexName.toString();
    }

    @Override
    public String getSchemaName() {
        return indexName.getSchemaName();
    }

    // constraint

    public TableName getConstraintName() {
        return constraintName;
    }

}
