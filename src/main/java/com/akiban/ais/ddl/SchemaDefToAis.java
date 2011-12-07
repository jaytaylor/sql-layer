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

package com.akiban.ais.ddl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.ddl.SchemaDef.CName;
import com.akiban.ais.ddl.SchemaDef.ColumnDef;
import com.akiban.ais.ddl.SchemaDef.IndexDef;
import com.akiban.ais.ddl.SchemaDef.IndexQualifier;
import com.akiban.ais.ddl.SchemaDef.ReferenceDef;
import com.akiban.ais.ddl.SchemaDef.UserTableDef;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.util.Strings;

/**
 * This class converts a SchemaDef object into an AIS. It currently uses the
 * AISBuilder class internally to do this.
 */
public class SchemaDefToAis {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaDefToAis.class.getName());
    private final SchemaDef schemaDef;
    private final AISBuilder builder;

    /**
     * Convert a SchemaDef into a brand new AIS instance.
     * @param schemaDef SchemaDef to convert into an AIS
     * @param akibandbOnly When <code>true</code>, only tables with the AKIBANDB engine are converted.
     * @throws Exception For any error encountered.
     */
    public SchemaDefToAis(SchemaDef schemaDef, boolean akibandbOnly) {
        this.schemaDef = schemaDef;
        this.builder = new AISBuilder();
        buildAISFromBuilder(akibandbOnly);
    }

    /**
     * Convert and add the contents of a SchemaDef into an existing AIS instance.
     * @param schemaDef SchemaDef to convert into an AIS
     * @param ais The AkibanInformationSchema to add to.
     * @param akibandbOnly When <code>true</code>, only tables with the AKIBANDB engine are converted.
     * @throws Exception For any error encountered.
     */
    public SchemaDefToAis(SchemaDef schemaDef, AkibanInformationSchema ais, boolean akibandbOnly) {
        this.schemaDef = schemaDef;
        this.builder = new AISBuilder(ais);
        buildAISFromBuilder(akibandbOnly);
    }

    public AkibanInformationSchema getAis() {
        return builder.akibanInformationSchema();
    }

    private static CName toCName(TableName tableName) {
        return new CName(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * Converted Akiban FKs into group relationships.
     */
    private void addImpliedGroups() {
        final Map<CName, SortedSet<CName>> groupMap = schemaDef.getGroupMap();
        groupMap.clear();
        // Already existing in AIS
        for (UserTable table : builder.akibanInformationSchema().getUserTables().values()) {
            // Add sets for existing groups but not any of the existing tables
            if (table.getParentJoin() == null) {
                final SortedSet<CName> members = new TreeSet<CName>();
                final TableName parentName = table.getName();
                groupMap.put(toCName(parentName), members);
            }
        }
        // Already existing in schemaDef
        final Set<CName> tablesInGroups = new HashSet<CName>();
        for (SortedSet<CName> tables : schemaDef.getGroupMap().values()) {
            tablesInGroups.addAll(tables);
        }
        for (final CName userTableName : schemaDef.getUserTableMap().keySet()) {
            final CName name = addImpliedGroupTable(tablesInGroups, userTableName);
            if (name == null) {
                LOG.warn("No Group for table {}", userTableName);
            }
        }
    }

    private CName addImpliedGroupTable(final Set<CName> tablesInGroups, final CName userTableName) {
        final UserTableDef utDef = schemaDef.getUserTableMap().get(userTableName);
        if (utDef != null && utDef.isAkibanTable() && !tablesInGroups.contains(userTableName)) {
            tablesInGroups.add(userTableName);
            List<ReferenceDef> joinRefs = utDef.getAkibanJoinRefs();
            if (joinRefs.isEmpty()) {
                // No joins, new root table. Default groupName is the root table name
                utDef.groupName = utDef.name;
                final SortedSet<CName> members = new TreeSet<CName>();
                members.add(userTableName);
                schemaDef.getGroupMap().put(utDef.groupName, members);
            } else {
                for (ReferenceDef refDef : joinRefs) {
                    utDef.parentName = addImpliedGroupTable(tablesInGroups, refDef.table);
                    if (utDef.parentName != null) {
                        UserTableDef parent = schemaDef.getUserTableMap().get(utDef.parentName);
                        utDef.groupName = parent.groupName;
                    }
                    else {
                        AkibanInformationSchema ais = builder.akibanInformationSchema();
                        UserTable parent = ais.getUserTable(refDef.table.getSchema(), refDef.table.getName());
                        if (parent != null) {
                            utDef.parentName = refDef.table;
                            utDef.groupName = toCName(parent.getGroup().getGroupTable().getRoot().getName());
                        }
                    }
                    if (utDef.groupName != null) {
                        schemaDef.getGroupMap().get(utDef.groupName).add(userTableName);
                    }
                }
            }
        }
        return utDef != null ? utDef.name : null;
    }

    private void removeNonAkibanForeignKeys() {
        for(UserTableDef userTableDef : schemaDef.getUserTableMap().values()) {
            if(userTableDef.isAkibanTable()) {
                for(IndexDef indexDef : userTableDef.indexes) {
                    if(indexDef.isForeignKey() && !indexDef.hasAkibanJoin()) {
                        indexDef.qualifiers.remove(IndexQualifier.FOREIGN_KEY);
                        indexDef.references.clear();
                    }
                }
            }
        }
    }

    private static class IdGenerator {
        private final Map<CName,CName> groupPerTable = new HashMap<CName, CName>();
        private final Map<CName,Integer> idPerGroup = new HashMap<CName, Integer>();

        IdGenerator(Map<CName,? extends Set<CName>> groupMap) {
            for(Map.Entry<CName, ? extends Set<CName>> entry : groupMap.entrySet()) {
                CName group = entry.getKey();
                for(CName uTable : entry.getValue()) {
                    groupPerTable.put(uTable, group);
                }
            }
        }

        public void setIdForGroupName(CName groupName, int id) {
            idPerGroup.put(groupName, id);
        }

        public int allocateId(CName uTableName) {
            CName groupTableName = groupPerTable.get(uTableName);
            Integer prevId = idPerGroup.get(groupTableName);
            if (prevId == null) {
                prevId = 0; // index ids start at 1
            }
            int id = prevId + 1;
            idPerGroup.put(groupTableName, id);
            return id;
        }
    }

    private static class GroupNamer {
        private final Set<String> names = new HashSet<String>();

        public String name(CName cname) {
            return name(cname.getName());
        }

        public String name(final String name) {
            if (names.add(name)) {
                return name;
            }

            int i = 0;
            StringBuilder builder = new StringBuilder(name).append('$');
            final int appendAt = builder.length();
            String ret;

            do {
                builder.setLength(appendAt);
                builder.append(i++);
            }
            while(!names.add(ret = builder.toString()));
            
            return ret;
        }
    }

    private int computeTableIdOffset(AkibanInformationSchema ais) {
        // Use 1 as default offset because the AAM uses tableID 0 as a marker value.
        int offset = 1;
        for(UserTable table : ais.getUserTables().values()) {
            if(!table.getName().getSchemaName().equals("akiban_information_schema")) {
                offset = Math.max(offset, table.getTableId() + 1);
            }
        }
        return offset;
    }

    private List<CName> depthFirstSortedUserTables(final CName groupName) {
        final LinkedList<CName> tableList = new LinkedList<CName>();
        final SortedSet<CName> groupMapCopy = new TreeSet<CName>();
        groupMapCopy.addAll(schemaDef.getGroupMap().get(groupName));
        while (!groupMapCopy.isEmpty()) {
            int startSize = tableList.size();
            Iterator<CName> it = groupMapCopy.iterator();
            while (it.hasNext()) {
                CName tableName = it.next();
                final UserTableDef utdef = schemaDef.getUserTableMap().get(tableName);
                assert utdef != null : tableName;
                if (utdef.parentName == null) {
                    tableList.add(0, tableName); // root table, beginning
                    it.remove();
                }
                else {
                    int insertIndex = tableList.indexOf(utdef.parentName);
                    if(insertIndex >= 0) {
                        tableList.add(insertIndex + 1, tableName); // after parent
                        it.remove();
                    }
                }
            }
            if (tableList.size() == startSize) {
                // No tables were added to the sorted list, assume parent(s) are in AIS
                tableList.addAll(groupMapCopy);
                groupMapCopy.clear();
            }
        }
        return tableList;
    }

    /**
     * Ensure any newly generated IDs are low to high according depth first order.
     * @param depthFirstTableList Table names in the order their IDs should be.
     */
    private void reassignTableIDsDepthFirst(List<CName> depthFirstTableList)
    {
        SortedSet<Integer> newIDs = new TreeSet<Integer>();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        for(CName tableName : depthFirstTableList) {
            UserTable table = ais.getUserTable(tableName.getSchema(), tableName.getName());
            boolean wasNew = newIDs.add(table.getTableId());
            assert wasNew : table.getTableId();
        }
        Iterator<Integer> idIt = newIDs.iterator();
        for(CName tableName : depthFirstTableList) {
            UserTable table = ais.getUserTable(tableName.getSchema(), tableName.getName());
            table.setTableId(idIt.next());
        }
    }

    private void buildAISFromBuilder(final boolean akibandbOnly) {
        removeNonAkibanForeignKeys();
        addImpliedGroups();

        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        builder.setTableIdOffset(computeTableIdOffset(ais));
        IdGenerator indexIdGenerator = new IdGenerator(schemaDef.getGroupMap());

        // Index IDs must be unique in a given group. For now, find maxes for all
        // groups. Really only need to find max for any groups in the SchemaDef.
        for(Group group : ais.getGroups().values()) {
            final UserTable root = group.getGroupTable().getRoot();
            final int maxId = findMaxIndexIDInGroup(ais, group);
            indexIdGenerator.setIdForGroupName(toCName(root.getName()), maxId);
        }

        // loop through user tables and add to AIS
        for (UserTableDef utDef : schemaDef.getUserTableMap().values()) {
            if (akibandbOnly && !utDef.isAkibanTable()) {
                continue;
            }
            String schemaName = utDef.getCName().getSchema();
            String tableName = utDef.getCName().getName();

            // table
            builder.userTable(schemaName, tableName);

            // engine
            UserTable ut = ais.getUserTable(schemaName, tableName);
            ut.setEngine(utDef.engine);
            ut.setCharset(utDef.charset);
            ut.setCollation(utDef.collate);

            // columns
            for (ColumnDef def : utDef.columns) {
                Type type = ais.getType(def.typeName);
                Column column = Column.create(ut, def.name, def.uposition, type);
                column.setNullable(def.nullable);
                column.setAutoIncrement(def.autoincrement != null);
                column.setTypeParameter1(longValue(def.typeParam1));
                column.setTypeParameter2(longValue(def.typeParam2));
                column.setCharset(def.charset);
                column.setCollation(def.collate);
                column.setInitialAutoIncrementValue(def.defaultAutoIncrement());
            }

            // pk index
            if (utDef.primaryKey != null) {
                final String name = utDef.primaryKey.getName();
                final int id = indexIdGenerator.allocateId(utDef.name);

                assert name.equals(Index.PRIMARY_KEY_CONSTRAINT) : utDef.primaryKey;
                final Index pkIndex = TableIndex.create(ais, ut, name, id, true, name);

                int columnIndex = 0;
                for (SchemaDef.IndexColumnDef indexColDef : utDef.primaryKey.columns) {
                    final Column column = ut.getColumn(indexColDef.columnName);
                    pkIndex.addColumn(new IndexColumn(pkIndex, column, columnIndex++,
                                                      !indexColDef.descending,
                                                      indexColDef.indexedLength));
                }
            }

            // indexes / constraints
            for (IndexDef indexDef : utDef.indexes) {
                String indexType = "KEY";
                boolean unique = false;
                for (SchemaDef.IndexQualifier qualifier : indexDef.qualifiers) {
                    if (qualifier.equals(SchemaDef.IndexQualifier.FOREIGN_KEY)) {
                        indexType = "FOREIGN KEY";
                    }
                    if (qualifier.equals(SchemaDef.IndexQualifier.UNIQUE)) {
                        indexType = "UNIQUE";
                        unique = true;
                    }
                }

                if (indexType.equalsIgnoreCase("FOREIGN KEY")) {
                    final CName childTable = utDef.name;
                    final List<String> childColumns = indexDef.getColumnNames();

                    // foreign keys (aka candidate joins)
                    for (ReferenceDef refDef : indexDef.references) {
                        CName parentTable = refDef.table;
                        String joinName = constructFKJoinName(utDef, refDef);

                        builder.joinTables(joinName, parentTable.getSchema(),
                                parentTable.getName(), childTable.getSchema(),
                                childTable.getName());

                        Iterator<String> childJoinColumnNameScan = childColumns.iterator();
                        Iterator<String> parentJoinColumnNameScan = refDef.columns.iterator();

                        while (childJoinColumnNameScan.hasNext()
                                && parentJoinColumnNameScan.hasNext()) {
                            String childJoinColumnName = childJoinColumnNameScan.next();
                            String parentJoinColumnName = parentJoinColumnNameScan.next();

                            builder.joinColumns(joinName, parentTable.getSchema(),
                                    parentTable.getName(), parentJoinColumnName,
                                    childTable.getSchema(), childTable.getName(), 
                                    childJoinColumnName);
                        }
                    }
                }

                // indexes
                Index fkIndex = TableIndex.create(ais, ut, indexDef.name,
                        indexIdGenerator.allocateId(utDef.name), unique, indexType);

                int columnIndex = 0;
                for (SchemaDef.IndexColumnDef indexColumnDef : indexDef.columns) {
                    Column fkColumn = ut.getColumn(indexColumnDef.columnName);
                    fkIndex.addColumn(new IndexColumn(fkIndex, fkColumn,
                            columnIndex++, !indexColumnDef.descending,
                            indexColumnDef.indexedLength));
                    
                }
            }
        }
        builder.basicSchemaIsComplete();

        GroupNamer namer = new GroupNamer();
        // Add existing group names
        for (String groupName : builder.akibanInformationSchema().getGroups().keySet()) {
            namer.name(groupName);
        }
        // loop through group tables and add to AIS
        for (CName group : schemaDef.getGroupMap().keySet()) {
            String groupName = null;
            List<CName> tablesInGroup = depthFirstSortedUserTables(group);
            reassignTableIDsDepthFirst(tablesInGroup);

            for (CName table : tablesInGroup) {
                UserTableDef tableDef = schemaDef.getUserTableMap().get(table);
                List<ReferenceDef> joinDefs = tableDef.getAkibanJoinRefs();
                if (joinDefs.isEmpty()) {
                        // New root table
                        LOG.debug("Group Root Table = {}", table.getName());
                        groupName = namer.name(group);
                        builder.createGroup(groupName, group.getSchema(), groupTableName(group));
                        builder.addTableToGroup(groupName, table.getSchema(), table.getName());
                }
                else {
                    if (groupName == null) {
                        // Not in tableList = wasn't in schemaDef, must be in AIS
                        final CName parentName = tableDef.parentName;
                        final UserTable parent = ais.getUserTable(parentName.getSchema(), parentName.getName());
                        groupName = parent.getGroup().getName();
                    }
                    LOG.debug("Group = {}", groupName);
                    for (ReferenceDef refDef : joinDefs) {
                        LOG.debug("Group Child Table = {}", tableDef.name.getName());
                        String joinName = constructFKJoinName(tableDef, refDef);
                        builder.addJoinToGroup(groupName, joinName, 0);
                    }
                }
            }
        }
        if (!schemaDef.getGroupMap().isEmpty()) {
            builder.groupingIsComplete();
        }
    }

    private String constructFKJoinName(UserTableDef childTable, ReferenceDef refDef) {
        String ret = String.format("%s/%s/%s/%s/%s/%s",
                                   refDef.table.getSchema(),
                                   refDef.table.getName(),
                                   Strings.join(refDef.columns, ","),
                                   childTable.getCName().getSchema(),
                                   childTable.name,
                                   Strings.join(refDef.index.getColumnNames(), ","));
        return ret.toLowerCase().replace(',', '_');
    }

    private Long longValue(final String s) {
        return s == null ? null : Long.parseLong(s);
    }

    private String groupTableName(final CName group) {
        return "_akiban_" + group.getName();
    }

    /**
     * Find the maximum index ID from all of the indexes within the given group.
     */
    public static int findMaxIndexIDInGroup(AkibanInformationSchema ais, Group group) {
        int maxId = Integer.MIN_VALUE;
        for(UserTable table : ais.getUserTables().values()) {
            if(table.getGroup().equals(group)) {
                for(Index index : table.getIndexesIncludingInternal()) {
                    maxId = Math.max(index.getIndexId(), maxId);
                }
            }
        }
        for(Index index : group.getIndexes()) {
            maxId = Math.max(index.getIndexId(), maxId);
        }
        return maxId;
    }
}
