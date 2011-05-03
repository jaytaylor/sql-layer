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

import java.util.ArrayList;
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
 * This class converts the SchemaDef object created by parsing DDL statements
 * into an AIS instance. The caller can choose whether to include tables for all
 * storage engine types, or just those marked "engine=akibandb". The latter
 * choice is used by the server component since it does not care about
 * non-akibandb tables. However, the studio component needs to review tables for
 * other engines.
 * 
 * @author peter
 * 
 */
public class SchemaDefToAis {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaDefToAis.class
            .getName());

    private final SchemaDef schemaDef;
    private final AkibanInformationSchema ais;

    public SchemaDefToAis(final SchemaDef schemaDef, final boolean akibandbOnly)
            throws Exception {
        this.schemaDef = schemaDef;
        this.ais = buildAISFromBuilder(akibandbOnly);
    }

    public AkibanInformationSchema getAis() {
        return ais;
    }

    /**
     * Converted Akiban FKs into group relationships.
     */
    private void addImpliedGroups() {
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
                // No joins, new root table. Create a new Group named as the table.
                utDef.groupName = userTableName;
                final SortedSet<CName> members = new TreeSet<CName>();
                members.add(userTableName);
                schemaDef.getGroupMap().put(utDef.groupName, members);
            } else {
                for (ReferenceDef refDef : joinRefs) {
                    utDef.parentName = addImpliedGroupTable(tablesInGroups, refDef.table);
                    if (utDef.parentName != null) {
                        UserTableDef parent = schemaDef.getUserTableMap().get(utDef.parentName);
                        utDef.groupName = parent.groupName;
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

    private List<CName> depthFirstSortedUserTables(final CName groupName) {
        final LinkedList<CName> tableList = new LinkedList<CName>();
        for (CName tableName : schemaDef.getGroupMap().get(groupName)) {
            final UserTableDef utdef = schemaDef.getUserTableMap().get(tableName);
            if (utdef.parentName == null) {
                tableList.add(0, tableName); // root table, beginning
            }
            else {
                int insertIndex = tableList.indexOf(utdef.parentName);
                if(insertIndex < 0) {
                    tableList.add(tableName); // parent not found, end
                }
                else {
                    tableList.add(insertIndex + 1, tableName); // after parent
                }
            }

        }
        return tableList;
    }

    private AkibanInformationSchema buildAISFromBuilder(final boolean akibandbOnly) throws Exception {
        removeNonAkibanForeignKeys();
        addImpliedGroups();

        AISBuilder builder = new AISBuilder();
        // Use 1 as default offset because the AAM uses tableID 0 as 
        // a marker value. 
        builder.setTableIdOffset(1);
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        IdGenerator indexIdGenerator = new IdGenerator(schemaDef.getGroupMap());

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
                final Index pkIndex = Index.create(ais, ut, name, id, true, name);

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
                Index fkIndex = Index.create(ais, ut, indexDef.name,
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

        // Add existing group names
        GroupNamer namer = new GroupNamer();
        // loop through group tables and add to AIS
        for (CName group : schemaDef.getGroupMap().keySet()) {
            final String groupName = namer.name(group);
            LOG.debug("Group = {}" + groupName);
            List<CName> tablesInGroup = depthFirstSortedUserTables(group);
            for (CName table : tablesInGroup) {
                UserTableDef tableDef = schemaDef.getUserTableMap().get(table);
                List<ReferenceDef> joinDefs = tableDef.getAkibanJoinRefs();
                if (joinDefs.isEmpty()) {
                    LOG.debug("Group Root Table = {} ", table.getName());
                    builder.createGroup(groupName, group.getSchema(), groupTableName(group));
                    builder.addTableToGroup(groupName, table.getSchema(), table.getName());
                }
                else {
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

        return builder.akibanInformationSchema();
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
}
