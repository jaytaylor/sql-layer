/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import java.util.*;

import com.foundationdb.server.rowdata.RowDef;

public abstract class Table extends Columnar implements Traversable, HasGroup
{
    public abstract boolean isUserTable();

    @Override
    public boolean isView() {
        return false;
    }

    protected Table(AkibanInformationSchema ais, String schemaName, String tableName, Integer tableId)
    {
        super(ais, schemaName, tableName);
        this.tableId = tableId;

        this.groupIndexes = new HashSet<>();
        this.unmodifiableGroupIndexes = Collections.unmodifiableCollection(groupIndexes);
        this.indexMap = new TreeMap<>();
        this.unmodifiableIndexMap = Collections.unmodifiableMap(indexMap);
    }

    public boolean isGroupTable()
    {
        return !isUserTable();
    }

    public Integer getTableId()
    {
        return tableId;
    }

    /**
     * Temporary mutator so that prototype AIS management can renumber all
     * the tables once created.  Longer term we want to give the table
     * its ID when generated.
     *
     * @param tableId
     */
    public void setTableId(final int tableId)
    {
        this.tableId = tableId;
    }

    public Integer getOrdinal()
    {
        return ordinal;
    }

    public void setOrdinal(Integer ordinal)
    {
        this.ordinal = ordinal;
    }

    public Group getGroup()
    {
        return group;
    }

    public void setGroup(Group group)
    {
        this.group = group;
    }

    public Collection<TableIndex> getIndexes()
    {
        return unmodifiableIndexMap.values();
    }

    public TableIndex getIndex(String indexName)
    {
        return unmodifiableIndexMap.get(indexName.toLowerCase());
    }

    /**
     * Get all GroupIndexes this table participates in, both explicit and implicit (i.e. as a declared column or
     * as an ancestor of a declared column
     */
    public final Collection<GroupIndex> getGroupIndexes() {
        return unmodifiableGroupIndexes;
    }

    protected void addIndex(TableIndex index)
    {
        indexMap.put(index.getIndexName().getName().toLowerCase(), index);
    }

    void clearIndexes() {
        indexMap.clear();
    }

    final void addGroupIndex(GroupIndex groupIndex) {
        groupIndexes.add(groupIndex);
    }

    final void removeGroupIndex(GroupIndex groupIndex) {
        groupIndexes.remove(groupIndex);
    }

    public void removeIndexes(Collection<TableIndex> indexesToDrop) {
        indexMap.values().removeAll(indexesToDrop);
    }

    /**
     * @deprecated - use AkibanInfomationSchema#validate() instead
     * @param out
     */
    public void checkIntegrity(List<String> out)
    {
        if (tableName == null) {
            out.add("table had null table name");
        }
        for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
            String name = entry.getKey();
            Column column = entry.getValue();
            if (column == null) {
                out.add("null column for name: " + name);
            } else if (name == null) {
                out.add("null name for column: " + column);
            } else if (!name.equals(column.getName())) {
                out.add("name mismatch, expected <" + name + "> for column " + column);
            }
        }
        if (!columnsStale) {
            for (Column column : columns) {
                if (column == null) {
                    out.add("null column in columns list");
                } else if (!columnMap.containsKey(column.getName())) {
                    out.add("columns not stale, but map didn't contain column: " + column.getName());
                }
            }
        }
        for (Map.Entry<String, TableIndex> entry : indexMap.entrySet()) {
            String name = entry.getKey();
            TableIndex index = entry.getValue();
            if (name == null) {
                out.add("null name for index: " + index);
            } else if (index == null) {
                out.add("null index for name: " + name);
            } else if (index.getTable() != this) {
                out.add("table's index.getTable() wasn't the table" + index + " <--> " + this);
            }
            if (index != null) {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    if (!index.equals(indexColumn.getIndex())) {
                        out.add("index's indexColumn.getIndex() wasn't index: " + indexColumn);
                    }
                    Column column = indexColumn.getColumn();
                    if (!columnMap.containsKey(column.getName())) {
                        out.add("index referenced a column not in the table: " + column);
                    }
                }
            }
        }
    }

    public void rowDef(RowDef rowDef)
    {
        this.rowDef = rowDef;
    }

    public RowDef rowDef()
    {
        return rowDef;
    }

    // State
    private final Map<String, TableIndex> indexMap;
    private final Map<String, TableIndex> unmodifiableIndexMap;
    private final Collection<GroupIndex> groupIndexes;
    private final Collection<GroupIndex> unmodifiableGroupIndexes;

    protected Group group;
    private Integer tableId;
    private RowDef rowDef;
    private Integer ordinal;
}
