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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Index implements Serializable, ModelNames, Traversable
{
    public abstract HKey hKey();
    public abstract boolean isTableIndex();
    public abstract void computeFieldAssociations(Map<Table,Integer> ordinalMap);

    
    public static Index create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        Index index = null;
        String schemaName = (String) map.get(index_schemaName);
        String tableName = (String) map.get(index_tableName);
        String indexType = (String) map.get(index_indexType);
        String indexName = (String) map.get(index_indexName);
        Integer indexId = (Integer) map.get(index_indexId);
        Boolean unique = (Boolean) map.get(index_unique);
        String constraint = (String) map.get(index_constraint);
        if(IndexType.TABLE.toString().equals(indexType)) {
            Table table = ais.getTable(schemaName, tableName);
            if (table != null) {
                index = TableIndex.create(ais, table, indexName, indexId, unique, constraint);
            }
        }
        else if(IndexType.GROUP.toString().equals(indexType)) {
            Group group = ais.getGroup(tableName);
            if (group != null) {
                index = GroupIndex.create(ais, group, indexName, indexId, unique, constraint);
            }
        }
        return index;
    }

    protected Index(TableName tableName, String indexName, Integer indexId, Boolean isUnique, String constraint)
    {
        this.indexName = new IndexName(tableName, indexName);
        this.indexId = indexId;
        this.isUnique = isUnique;
        this.constraint = constraint;
        columns = new ArrayList<IndexColumn>();
    }

    public boolean isGroupIndex()
    {
        return !isTableIndex();
    }

    @SuppressWarnings("unused")
    protected Index()
    {
        // GWT
    }

    @Override
    public String toString()
    {
        return "Index(" + indexName + columns + ")";
    }

    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(index_schemaName, indexName.getSchemaName());
        map.put(index_tableName, indexName.getTableName());
        map.put(index_indexType, getIndexType().toString());
        map.put(index_indexName, indexName.getName());
        map.put(index_indexId, indexId);
        map.put(index_unique, isUnique);
        map.put(index_constraint, constraint);
        return map;
    }

    public void addColumn(IndexColumn indexColumn)
    {
        if (columnsFrozen) {
            throw new IllegalStateException("can't add column because columns list is frozen");
        }
        columns.add(indexColumn);
        columnsStale = true;
    }

    public void freezeColumns() {
        if (!columnsFrozen) {
            sortColumnsIfNeeded();
            columnsFrozen = true;
        }
    }

    public abstract Table leafMostTable();
    public abstract Table rootMostTable();

    public boolean isUnique()
    {
        return isUnique;
    }

    public boolean isPrimaryKey()
    {
        return constraint.equals(PRIMARY_KEY_CONSTRAINT);
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

    public List<IndexColumn> getColumns()
    {
        sortColumnsIfNeeded();
        return columns;
    }

    private void sortColumnsIfNeeded() {
        if (columnsStale) {
            Collections.sort(columns,
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
    public void traversePreOrder(Visitor visitor) throws Exception
    {
        for (IndexColumn indexColumn : getColumns()) {
            visitor.visitIndexColumn(indexColumn);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor) throws Exception
    {
        traversePreOrder(visitor);
    }

    public Object indexDef()
    {
        return indexDef;
    }

    public void indexDef(Object indexDef)
    {
        assert indexDef.getClass().getName().equals("com.akiban.server.IndexDef") : indexDef.getClass();
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

    public IndexToHKey indexToHKey()
    {
        return indexToHKey;
    }

    public boolean isHKeyEquivalent()
    {
        return isHKeyEquivalent;
    }

    private static class AssociationBuilder {
        /**
         * @param i1 Entry of {@link IndexRowComposition#depths} <b>or</b> {@link IndexToHKey#ordinals}
         * @param i2 Entry of {@link IndexRowComposition#fieldPositions} <b>or</b> {@link IndexToHKey#indexRowPositions}
         * @param i3 Entry of {@link IndexRowComposition#hkeyPositions} <b>or</b> {@link IndexToHKey#fieldPositions}
         */
        void addEntry(int i1, int i2, int i3) {
            list1.add(i1); list2.add(i2); list3.add(i3);
        }

        IndexRowComposition createIndexRowComposition() {
            return new IndexRowComposition(asArray(list1), asArray(list2), asArray(list3));
        }

        IndexToHKey createIndexToHKey() {
            return new IndexToHKey(asArray(list1), asArray(list2), asArray(list3));
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
        private List<Integer> list3 = new ArrayList<Integer>();
    }
    
    /**
     * Note: Probably needs list/array of tables indicating depth to support group indexes
     * @param ordinalMap Map of Tables to ordinal values
     * @param indexTable Table the HKey is referencing
     */
    protected void internalComputeFieldAssociations(Map<Table,Integer> ordinalMap, Table indexTable) {
        freezeColumns();
        computeHKeyEquivalent();

        AssociationBuilder rowCompBuilder = new AssociationBuilder();
        AssociationBuilder toHKeyBuilder = new AssociationBuilder();
        List<Column> indexColumns = new ArrayList<Column>();

        // Add index key fields
        for (IndexColumn iColumn : getColumns()) {
            Column column = iColumn.getColumn();
            indexColumns.add(column);
            rowCompBuilder.addEntry(-1, column.getPosition(), -1);
        }

        // Add hkey fields not already included
        HKey hKey = hKey();
        for (HKeySegment hKeySegment : hKey.segments()) {
            Integer ordinal = ordinalMap.get(hKeySegment.table());
            assert ordinal != null : hKeySegment.table();
            toHKeyBuilder.addEntry(ordinal, -1, -1);

            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column column = hKeyColumn.column();

                if (!indexColumns.contains(column)) {
                    if (indexTable.getColumnsIncludingInternal().contains(column)) {
                        rowCompBuilder.addEntry(-1, hKeyColumn.column().getPosition(), -1);
                    } else {
                        assert hKeySegment.table().isUserTable() : this;
                        rowCompBuilder.addEntry(-1, -1, hKeyColumn.positionInHKey());
                    }
                    indexColumns.add(hKeyColumn.column());
                }

                int indexRowPos = indexColumns.indexOf(column);
                int fieldPos = column == null ? -1 : column.getPosition();
                toHKeyBuilder.addEntry(-1, indexRowPos, fieldPos);
            }
        }

        indexRowComposition = rowCompBuilder.createIndexRowComposition();
        indexToHKey = toHKeyBuilder.createIndexToHKey();
    }

    private void computeHKeyEquivalent() {
        isHKeyEquivalent = false;
        /*
        isHKeyEquivalent = true;
        // Collect the HKeyColumns of the index's hkey
        List<HKeyColumn> hKeyColumns = new ArrayList<HKeyColumn>();
        for (HKeySegment hKeySegment : index.hKey().segments()) {
            hKeyColumns.addAll(hKeySegment.columns());
        }
        // Scan hkey columns and index columns and see if they match
        Iterator<HKeyColumn> hKeyColumnScan = hKeyColumns.iterator();
        Iterator<IndexColumn> indexColumnScan = index.getColumns().iterator();
        while (hkeyEquivalent && hKeyColumnScan.hasNext() && indexColumnScan.hasNext()) {
            Column hKeyColumn = hKeyColumnScan.next().column();
            Column indexColumn = indexColumnScan.next().getColumn();
            isHKeyEquivalent = hKeyColumn == indexColumn;
        }
        if (hkeyEquivalent && !hKeyColumnScan.hasNext() && indexColumnScan.hasNext()) {
            isHKeyEquivalent = false;
        }
        */
    }

    public static final String PRIMARY_KEY_CONSTRAINT = "PRIMARY";
    public static final String UNIQUE_KEY_CONSTRAINT = "UNIQUE";
    public static final String KEY_CONSTRAINT = "KEY";

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


    private IndexName indexName;
    private Integer indexId;
    private Boolean isUnique;
    private String constraint;
    private boolean columnsStale = true;
    private List<IndexColumn> columns;
    private boolean columnsFrozen = false;

    // It really is an IndexDef, but declaring it that way creates trouble for AIS. We don't want to pull in
    // all the RowDef stuff and have it visible to GWT.
    private transient /* IndexDef */ Object indexDef;
    private transient IndexRowComposition indexRowComposition;
    private transient IndexToHKey indexToHKey;
    private transient boolean isHKeyEquivalent;
}
