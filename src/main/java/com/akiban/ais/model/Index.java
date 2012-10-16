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
import com.akiban.qp.persistitadapter.SpatialHelper;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.geophile.Space;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.persistit.Tree;

import java.util.*;

public abstract class Index implements Traversable
{
    public abstract HKey hKey();
    public abstract boolean isTableIndex();
    public abstract void computeFieldAssociations(Map<Table,Integer> ordinalMap);
    public abstract Table leafMostTable();
    public abstract Table rootMostTable();
    public abstract void checkMutability();

    protected Index(TableName tableName,
                    String indexName,
                    Integer indexId,
                    Boolean isUnique,
                    String constraint,
                    JoinType joinType,
                    boolean isValid)
    {
        if ( (indexId != null) && (indexId | INDEX_ID_BITS) != INDEX_ID_BITS)
            throw new IllegalArgumentException("index ID out of range: " + indexId + " > " + INDEX_ID_BITS);
        AISInvariants.checkNullName(indexName, "index", "index name");

        this.indexName = new IndexName(tableName, indexName);
        this.indexId = indexId;
        this.isUnique = isUnique;
        this.constraint = constraint;
        this.joinType = joinType;
        this.isValid = isValid;
        keyColumns = new ArrayList<IndexColumn>();
    }

    protected Index(TableName tableName, String indexName, Integer idAndFlags, Boolean isUnique, String constraint) {
        this (
                tableName,
                indexName,
                extractIndexId(idAndFlags),
                isUnique,
                constraint,
                extractJoinType(idAndFlags),
                extractIsValid(idAndFlags)
        );
    }

    public boolean isGroupIndex()
    {
        return !isTableIndex();
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public boolean isValid() {
        return isTableIndex() || isValid;
    }

    @Override
    public String toString()
    {
        return "Index(" + indexName + keyColumns + ")";
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
        return constraint.equals(PRIMARY_KEY_CONSTRAINT);
    }

    public boolean isAkibanForeignKey() {
        return constraint.equals(FOREIGN_KEY_CONSTRAINT) &&
               indexName.getName().startsWith(GROUPING_FK_PREFIX);
    }

    public String getConstraint()
    {
        return constraint;
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

    public IndexMethod getIndexMethod() {
        return IndexMethod.NORMAL;
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

    @Override
    public void traversePreOrder(Visitor visitor)
    {
        for (IndexColumn indexColumn : getKeyColumns()) {
            visitor.visitIndexColumn(indexColumn);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor)
    {
        traversePreOrder(visitor);
    }

    public IndexDef indexDef()
    {
        return indexDef;
    }

    public void indexDef(IndexDef indexDef)
    {
        assert indexDef.getClass().getName().equals("com.akiban.server.rowdata.IndexDef") : indexDef.getClass();
        this.indexDef = indexDef;
    }

    public final IndexType getIndexType()
    {
        return isTableIndex() ? IndexType.TABLE : IndexType.GROUP;
    }

    public IndexRowComposition indexRowComposition()
    {
        return indexRowComposition;
    }

    public boolean isUniqueAndMayContainNulls()
    {
        return false;
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

        private List<Integer> list1 = new ArrayList<Integer>();
        private List<Integer> list2 = new ArrayList<Integer>();
    }
    
    private static JoinType extractJoinType(Integer idAndFlags) {
        if (idAndFlags == null)
            return  null;
        return (idAndFlags & IS_RIGHT_JOIN_FLAG) == IS_RIGHT_JOIN_FLAG
                ? JoinType.RIGHT
                : JoinType.LEFT;
    }

    private static boolean extractIsValid(Integer idAndFlags) {
        return idAndFlags != null && (idAndFlags & IS_VALID_FLAG) == IS_VALID_FLAG;
    }

    private static Integer extractIndexId(Integer idAndFlags) {
        if (idAndFlags == null)
            return null;
        return idAndFlags & INDEX_ID_BITS;
    }

    public Integer getIdAndFlags() {
        if(indexId == null) {
            return null;
        }
        int idAndFlags = indexId;
        if(isValid) {
            idAndFlags |= IS_VALID_FLAG;
        }
        if(joinType == JoinType.RIGHT) {
            idAndFlags |= IS_RIGHT_JOIN_FLAG;
        }
        return idAndFlags;
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

    // Unique, non-PK indexes store a "null separator value", making index rows unique that would otherwise
    // be considered duplicates due to nulls.
    public long nextNullSeparatorValue(TreeService treeService)
    {
        Tree tree = indexDef.getTreeCache().getTree();
        AccumulatorAdapter accumulator =
            new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.UNIQUE_ID, treeService, tree);
        return accumulator.updateAndGet(1);
    }

    // akTypes, akCollators and tInstances provide type info for physical index rows.
    // Physical != logical for spatial indexes.

    public AkType[] akTypes()
    {
        ensureTypeInfo();
        return akTypes;
    }

    public AkCollator[] akCollators()
    {
        ensureTypeInfo();
        return akCollators;
    }

    public TInstance[] tInstances()
    {
        ensureTypeInfo();
        return tInstances;
    }

    private void ensureTypeInfo()
    {
        if (akTypes == null && tInstances == null) {
            synchronized (this) {
                if (akTypes == null && tInstances == null) {
                    int physicalColumns;
                    int firstSpatialColumn;
                    int dimensions;
                    if (isSpatial()) {
                        TableIndex spatialIndex = (TableIndex) this;
                        dimensions = spatialIndex.dimensions();
                        physicalColumns = allColumns.size() - dimensions + 1;
                        firstSpatialColumn = spatialIndex.firstSpatialArgument();
                    } else {
                        dimensions = 0;
                        physicalColumns = allColumns.size();
                        firstSpatialColumn = Integer.MAX_VALUE;
                    }
                    AkType[] localAkTypes = new AkType[physicalColumns];
                    AkCollator[] localAkCollators = new AkCollator[physicalColumns];
                    TInstance[] localTInstances = new TInstance[physicalColumns];
                    int logicalColumn = 0;
                    int physicalColumn = 0;
                    int nColumns = allColumns.size();
                    while (logicalColumn < nColumns) {
                        if (logicalColumn == firstSpatialColumn) {
                            if (Types3Switch.ON) {
                                localTInstances[physicalColumn] =
                                    MNumeric.BIGINT.instance(SpatialHelper.isNullable(this));
                            } else {
                                localAkTypes[physicalColumn] = AkType.LONG;
                                localAkCollators[physicalColumn] = null;
                            }
                            logicalColumn += dimensions;
                        } else {
                            IndexColumn indexColumn = allColumns.get(logicalColumn);
                            Column column = indexColumn.getColumn();
                            if (Types3Switch.ON) {
                                localTInstances[physicalColumn] = column.tInstance();
                            } else {
                                localAkTypes[physicalColumn] = column.getType().akType();
                                localAkCollators[physicalColumn] = column.getCollator();
                            }
                            logicalColumn++;
                        }
                        physicalColumn++;
                    }
                    akCollators = localAkCollators;
                    tInstances = localTInstances;
                    akTypes = localAkTypes;
                }
            }
        }
    }

    public static boolean isSpatialCompatible(TableIndex index)
    {
        boolean isSpatialCompatible = false;
        List<IndexColumn> indexColumns = index.getKeyColumns();
        if (indexColumns.size() == Space.LAT_LON_DIMENSIONS) {
            isSpatialCompatible = true;
            for (int d = 0; d < index.dimensions(); d++) {
                isSpatialCompatible =
                    isSpatialCompatible &&
                    isFixedDecimal(indexColumns.get(index.firstSpatialArgument() + d).getColumn());
            }
        }
        return isSpatialCompatible;
    }

    private static boolean isFixedDecimal(Column column) {
        if (Types3Switch.ON) {
            return column.tInstance().typeClass() instanceof MBigDecimal;
        } else {
            AkType type = column.getType().akType();
            return type == AkType.DECIMAL;
        }
    }

    public static final String PRIMARY_KEY_CONSTRAINT = "PRIMARY";
    public static final String UNIQUE_KEY_CONSTRAINT = "UNIQUE";
    public static final String KEY_CONSTRAINT = "KEY";
    public static final String FOREIGN_KEY_CONSTRAINT = "FOREIGN KEY";
    public static final String GROUPING_FK_PREFIX = "__akiban";

    private static final int INDEX_ID_BITS = 0x0000FFFF;
    private static final int IS_VALID_FLAG = INDEX_ID_BITS + 1;
    private static final int IS_RIGHT_JOIN_FLAG = IS_VALID_FLAG << 1;

    private final Boolean isUnique;
    private final String constraint;
    private final JoinType joinType;
    private final boolean isValid;
    private Integer indexId;
    private IndexName indexName;
    private boolean columnsStale = true;
    private boolean columnsFrozen = false;
    private String treeName;
    private IndexDef indexDef;
    protected IndexRowComposition indexRowComposition;
    protected List<IndexColumn> keyColumns;
    protected List<IndexColumn> allColumns;
    private volatile AkType[] akTypes;
    private volatile AkCollator[] akCollators;
    private volatile TInstance[] tInstances;

    public enum JoinType {
        LEFT, RIGHT
    }

    public static enum IndexType {
        TABLE("TABLE"),
        GROUP("GROUP")
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
        NORMAL, Z_ORDER_LAT_LON
    }

    public String getTreeName() {
        return treeName;
    }

    public void setTreeName(String treeName) {
        this.treeName = treeName;
    }

}
