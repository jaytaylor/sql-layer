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

package com.akiban.ais.model;

import java.util.*;

import com.akiban.server.rowdata.RowDef;

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

    public MigrationUsage getMigrationUsage() {
        return migrationUsage;
    }

    public void setMigrationUsage(MigrationUsage migrationUsage) {
        this.migrationUsage = migrationUsage;
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
     * <p>Our intended migration policy; the grouping algorithm must also take these values into account.</p>
     * <p/>
     * <p>The enums {@linkplain #KEEP_ENGINE} and {@linkplain #INCOMPATIBLE} have similar effects on grouping and
     * migration: tables marked with these values will not be included in any groups, and during migration, their
     * storage engine is not changed to AkibanDb. The difference between the two enums is that {@linkplain #KEEP_ENGINE}
     * is set by the user and can later be changed to {@linkplain #AKIBAN_STANDARD} or
     * {@linkplain @#AKIBAN_LOOKUP_TABLE}; on the other hand, {@linkplain #INCOMPATIBLE} is set during analysis and
     * signifies that migration will not work for this table. The user should not be able to set this flag, and if
     * this flag is set, the user should not be able to change it.</p>
     */
    public enum MigrationUsage
    {
        /**
         * Migrate this table to AkibanDb, grouping it as a standard user table. This is just a normal migration.
         */
        AKIBAN_STANDARD,
        /**
         * Migrate this table to AkibanDb, but as a lookup table. Lookup tables are grouped alone.
         */
        AKIBAN_LOOKUP_TABLE,
        /**
         * User wants to keep this table's engine as-is; don't migrate it to AkibanDb.
         */
        KEEP_ENGINE,
        /**
         * This table can't be migrated to AkibanDb.
         */
        INCOMPATIBLE;

        /**
         * Returns whether this usage requires an AkibanDB engine.
         *
         * @return whether this enum is one that requires AkibanDB
         */
        public boolean isAkiban()
        {
            return (this == AKIBAN_STANDARD) || (this == AKIBAN_LOOKUP_TABLE);
        }

        /**
         * <p>Returns whether this usage requires that the table participate in grouping.</p>
         * <p/>
         * <p>Tables participate in grouping if they're AkibanDB (see {@linkplain #isAkiban()} <em>and</em>
         * are not lookups.</p>
         *
         * @return
         */
        public boolean includeInGrouping()
        {
            return this == AKIBAN_STANDARD;
        }
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

    public String getEngine()
    {
        return engine;
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
    protected MigrationUsage migrationUsage = MigrationUsage.AKIBAN_STANDARD;
    protected String engine;
    private RowDef rowDef;
}
