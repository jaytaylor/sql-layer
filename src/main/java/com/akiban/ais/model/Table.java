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

import java.util.*;

import com.akiban.ais.model.validation.AISInvariants;

public abstract class Table implements Traversable, HasGroup
{
    public abstract boolean isUserTable();

    @Override
    public String toString()
    {
        return tableName.toString();
    }

    protected Table(AkibanInformationSchema ais, String schemaName, String tableName, Integer tableId)
    {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Table", "schema name");
        AISInvariants.checkNullName(tableName, "Table", "table name");
        AISInvariants.checkDuplicateTables(ais, schemaName, tableName);

        this.ais = ais;
        this.tableName = new TableName(schemaName, tableName);
        this.tableId = tableId;

        this.groupIndexes = new HashSet<GroupIndex>();
        this.unmodifiableGroupIndexes = Collections.unmodifiableCollection(groupIndexes);
        this.indexMap = new TreeMap<String, TableIndex>();
        this.unmodifiableIndexMap = Collections.unmodifiableMap(indexMap);
    }

    public AkibanInformationSchema getAIS()
    {
        return ais;
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

    public TableName getName()
    {
        return tableName;
    }

    public Group getGroup()
    {
        return group;
    }

    public void setGroup(Group group)
    {
        this.group = group;
    }

    public Column getColumn(String columnName)
    {
        return columnMap.get(columnName.toLowerCase());
    }

    public Column getColumn(Integer position)
    {
        return getColumns().get(position);
    }

    public List<Column> getColumns()
    {
        ensureColumnsUpToDate();
        return columnsWithoutInternal;
    }

    public List<Column> getColumnsIncludingInternal()
    {
        ensureColumnsUpToDate();
        return columns;
    }

    public Collection<TableIndex> getIndexes()
    {
        return unmodifiableIndexMap.values();
    }

    public TableIndex getIndex(String indexName)
    {
        return unmodifiableIndexMap.get(indexName.toLowerCase());
    }

    public final Collection<GroupIndex> getGroupIndexes() {
        return unmodifiableGroupIndexes;
    }

    public CharsetAndCollation getCharsetAndCollation()
    {
        return
            charsetAndCollation == null
            ? ais.getCharsetAndCollation()
            : charsetAndCollation;
    }

    public void setCharsetAndCollation(CharsetAndCollation charsetAndCollation)
    {
        this.charsetAndCollation = charsetAndCollation;
    }

    public void setCharset(String charset)
    {
        if (charset != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(charset, getCharsetAndCollation().collation());
        }
    }

    public void setCollation(String collation)
    {
        if (collation != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(getCharsetAndCollation().charset(), collation);
        }
    }

    public MigrationUsage getMigrationUsage() {
        return migrationUsage;
    }

    public void setMigrationUsage(MigrationUsage migrationUsage) {
        this.migrationUsage = migrationUsage;
    }

    public boolean isAISTable()
    {
        return tableName.getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA);
    }

    protected void addColumn(Column column)
    {
        columnMap.put(column.getName().toLowerCase(), column);
        columnsStale = true;
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

    protected void dropColumns()
    {
        columnMap.clear();
        columnsStale = true;
    }

    // For use by this package

    void setTableName(TableName tableName)
    {
        this.tableName = tableName;
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
     * check if this table belongs to a frozen AIS, 
     * throw exception if ais is frozen 
     */
    void checkMutability() {
        ais.checkMutability();
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
                for (IndexColumn indexColumn : index.getColumns()) {
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

    public void rowDef(Object rowDef)
    {
        assert rowDef.getClass().getName().equals("com.akiban.server.rowdata.RowDef") : rowDef.getClass();
        this.rowDef = rowDef;
    }

    public Object rowDef()
    {
        return rowDef;
    }

    private void ensureColumnsUpToDate()
    {
        if (columnsStale) {
            synchronized (columnsStaleLock) {
                if (columnsStale) {
                    columns.clear();
                    columns.addAll(columnMap.values());
                    Collections.sort(columns,
                                     new Comparator<Column>()
                                     {
                                         @Override
                                         public int compare(Column x, Column y)
                                         {
                                             return x.getPosition() - y.getPosition();
                                         }
                                     });
                    columnsWithoutInternal.clear();
                    for (Column column : columns) {
                        if (!column.isAkibanPKColumn()) {
                            columnsWithoutInternal.add(column);
                        }
                    }
                    columnsStale = false;
                }
            }
        }
    }

    public String getTreeName() {
        return treeName;
    }

    public void setTreeName(String treeName) {
        this.treeName = treeName;
    }

    // State

    protected Group group;
    protected TableName tableName;
    private Integer tableId;
    private volatile boolean columnsStale = true;
    private CharsetAndCollation charsetAndCollation;
    protected MigrationUsage migrationUsage = MigrationUsage.AKIBAN_STANDARD;
    protected String engine;
    protected String treeName;

    protected final AkibanInformationSchema ais;
    private final Object columnsStaleLock = new Object();
    private final List<Column> columns = new ArrayList<Column>();
    private final List<Column> columnsWithoutInternal = new ArrayList<Column>();
    private final Map<String, TableIndex> indexMap;
    private final Map<String, TableIndex> unmodifiableIndexMap;
    private final Map<String, Column> columnMap = new TreeMap<String, Column>();
    private final Collection<GroupIndex> groupIndexes;
    private final Collection<GroupIndex> unmodifiableGroupIndexes;

    // It really is a RowDef, but declaring it that way creates trouble for AIS. We don't want to pull in
    // all the RowDef stuff and have it visible to GWT.
    private /*RowDef*/ Object rowDef;
}
