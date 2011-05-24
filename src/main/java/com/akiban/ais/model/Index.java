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

    public abstract boolean isTableIndex();

    public boolean isGroupIndex()
    {
        return !isTableIndex();
    }

    public abstract HKey hKey();

    @SuppressWarnings("unused")
    private Index()
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
}
