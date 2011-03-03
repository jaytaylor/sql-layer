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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.akiban.ais.model.AkibanInformationSchema;
import org.antlr.runtime.RecognitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.ddl.SchemaDef.CName;
import com.akiban.ais.ddl.SchemaDef.ColumnDef;
import com.akiban.ais.ddl.SchemaDef.IndexDef;
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

    private final static String AKIBANDB_ENGINE_NAME = "akibandb";

    private final SchemaDef schemaDef;
    private final AkibanInformationSchema ais;

    public SchemaDefToAis(final SchemaDef schemaDef, final boolean akibandbOnly)
            throws RecognitionException, Exception {
        this.schemaDef = schemaDef;
        this.ais = buildAISFromBuilder(akibandbOnly);
    }

    public AkibanInformationSchema getAis() {
        return ais;
    }

    /**
     * Tests an FOREIGN KEY index definition to determine whether it represents
     * a group-defining relationship.
     * 
     * @param indexDef
     * @return
     */
    private static boolean isAkiban(final IndexDef indexDef) {
        return SchemaDef.isAkiban(indexDef);
    }

    private void computeColumnMapAndPositions() {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            int gposition = 0;
            final List<CName> tableList = depthFirstSortedUserTables(groupName);
            for (final CName tableName : tableList) {
                final UserTableDef utdef = schemaDef.getUserTableMap().get(
                        tableName);
                List<ColumnDef> columns = utdef.columns;
                for (final ColumnDef def : columns) {
                    def.gposition = gposition++;
                }
            }
        }
    }

    /**
     * Return List of names of user tables in a specified group sorted by
     * depth-first traversal of the parent-child relationships between those
     * tables.
     * 
     * @param groupName
     * @return sorted List of table names
     */
    private List<CName> depthFirstSortedUserTables(final CName groupName) {
        final CName root = new CName(schemaDef, "", "");
        final Map<CName, SortedSet<CName>> hierarchy = new HashMap<CName, SortedSet<CName>>();
        final List<CName> tableList = new ArrayList<CName>();
        for (final CName tableName : schemaDef.getGroupMap().get(groupName)) {
            final UserTableDef utdef = schemaDef.getUserTableMap().get(
                    tableName);
            CName parent = utdef.parent == null ? root : utdef.parent.name;
            SortedSet<CName> children = hierarchy.get(parent);
            if (children == null) {
                children = new TreeSet<CName>();
                hierarchy.put(parent, children);
            }
            children.add(utdef.name);
        }
        traverseDepthFirstSortedTableMap(root, hierarchy, tableList);
        if (tableList.isEmpty()) {
            throw new IllegalStateException("Broken user table hiearchy: "
                    + hierarchy);
        }
        return tableList;
    }

    private void traverseDepthFirstSortedTableMap(final CName parent,
            final Map<CName, SortedSet<CName>> hierarchy,
            final List<CName> tableList) {
        SortedSet<CName> siblings = hierarchy.get(parent);
        if (siblings != null) {
            for (final CName tableName : siblings) {
                tableList.add(tableName);
                traverseDepthFirstSortedTableMap(tableName, hierarchy,
                        tableList);
            }
        }
    }

    /**
     * Hack to convert annotated FK constraints into group relationships. For
     * now this "supplements" the group syntax in DDLSource.
     */
    private void addImpliedGroups() {
        final Set<CName> tablesInGroups = new HashSet<CName>();
        for (final Map.Entry<CName, SortedSet<CName>> entry : schemaDef
                .getGroupMap().entrySet()) {
            tablesInGroups.addAll(entry.getValue());
        }

        for (final CName userTableName : schemaDef.getUserTableMap().keySet()) {

            final UserTableDef utDef = addImpliedGroupTable(tablesInGroups,
                    userTableName);
            if (utDef == null) {
                LOG.warn("No Group for table " + userTableName);
            }
        }
    }

    private static IndexDef getAkibanJoin(UserTableDef table) {
        IndexDef annotatedFK = null;
        for (final IndexDef indexDef : table.indexes) {
            if (isAkiban(indexDef)) {
                // TODO: Fragile - could be two or nore of these
                assert annotatedFK == null : "previous annotated FK: "
                        + annotatedFK;
                annotatedFK = indexDef;
            }
        }
        return annotatedFK;
    }

    private UserTableDef addImpliedGroupTable(final Set<CName> tablesInGroups,
            final CName userTableName) {
        final UserTableDef utDef = schemaDef.getUserTableMap().get(
                userTableName);
        if (utDef != null && "akibandb".equalsIgnoreCase(utDef.engine)
                && !tablesInGroups.contains(userTableName)) {
            IndexDef annotatedFK = getAkibanJoin(utDef);
            if (annotatedFK == null) {
                // No FK: this is a new root table so create a new Group
                // By default the group has the same name is its root
                // user table.
                final CName groupName = userTableName;
                final SortedSet<CName> members = new TreeSet<CName>();
                schemaDef.getGroupMap().put(groupName, members);
                utDef.groupName = groupName;
                members.add(userTableName);
            } else {
                utDef.parent = addImpliedGroupTable(tablesInGroups,
                        annotatedFK.referenceTable);
                if (utDef.parent != null) {
                    utDef.groupName = utDef.parent.groupName;
                    for (SchemaDef.IndexColumnDef childColumn : annotatedFK.columns) {
                        utDef.childJoinColumns.add(childColumn.columnName);
                    }
                    utDef.parentJoinColumns
                            .addAll(annotatedFK.referenceColumns);
                    final SortedSet<CName> members = schemaDef.getGroupMap()
                            .get(utDef.groupName);
                    members.add(userTableName);
                }
            }
            tablesInGroups.add(userTableName);
        }
        return utDef;
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
                idPerGroup.put(groupTableName, 0);
                return 0;
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

    private AkibanInformationSchema buildAISFromBuilder(
            final boolean akibandbOnly) throws RecognitionException, Exception {
        addImpliedGroups();
        computeColumnMapAndPositions();

        AISBuilder builder = new AISBuilder();
        builder.setTableIdOffset(0);
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        IdGenerator indexIdGenerator = new IdGenerator(schemaDef.getGroupMap());

        // loop through user tables and add to AIS
        for (UserTableDef utDef : schemaDef.getUserTableMap().values()) {
            if (akibandbOnly
                    && !AKIBANDB_ENGINE_NAME.equalsIgnoreCase(utDef.engine)) {
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

            // auto-increment
            if (utDef.getAutoIncrementColumn() != null
                    && utDef.getAutoIncrementColumn().defaultAutoIncrement() != null) {
                ut.setInitialAutoIncrementValue(utDef.getAutoIncrementColumn()
                        .defaultAutoIncrement());
            }

            // columns
            List<ColumnDef> columns = utDef.columns;
            int columnIndex = 0;
            for (ColumnDef def : columns) {
                Type type = ais.getType(def.typeName);
                Column column = Column
                        .create(ut, def.name, def.uposition, type);
                column.setNullable(def.nullable);
                column.setAutoIncrement(def.autoincrement == null ? false
                        : true);
                column.setTypeParameter1(longValue(def.typeParam1));
                column.setTypeParameter2(longValue(def.typeParam2));
                column.setCharset(def.charset);
                column.setCollation(def.collate);
                column.setInitialAutoIncrementValue(def.defaultAutoIncrement());
            }

            // pk index
            if (utDef.primaryKey.size() > 0) {
                String pkIndexName = "PRIMARY";
                Index pkIndex = Index.create(ais, ut, pkIndexName,
                        indexIdGenerator.allocateId(utDef.name), true, pkIndexName);

                columnIndex = 0;
                for (String pkName : utDef.primaryKey) {
                    Column pkColumn = ut.getColumn(pkName);
                    pkIndex.addColumn(new IndexColumn(pkIndex, pkColumn,
                            columnIndex++, true, null));
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
                    // foreign keys (aka candidate joins)
                    CName childTable = utDef.name;
                    CName parentTable = indexDef.referenceTable;
                    String joinName = constructFKJoinName(utDef, indexDef);

                    builder.joinTables(joinName, parentTable.getSchema(),
                            parentTable.getName(), childTable.getSchema(),
                            childTable.getName());

                    Iterator<String> childJoinColumnNameScan = indexDef
                            .getChildColumns().iterator();
                    Iterator<String> parentJoinColumnNameScan = indexDef
                            .getParentColumns().iterator();

                    while (childJoinColumnNameScan.hasNext()
                            && parentJoinColumnNameScan.hasNext()) {
                        String childJoinColumnName = childJoinColumnNameScan
                                .next();
                        String parentJoinColumnName = parentJoinColumnNameScan
                                .next();

                        builder.joinColumns(joinName, parentTable.getSchema(),
                                parentTable.getName(), parentJoinColumnName,
                                childTable.getSchema(), childTable.getName(),
                                childJoinColumnName);
                    }
                }
                // else
                {
                    // indexes
                    Index fkIndex = Index.create(ais, ut, indexDef.name,
                            indexIdGenerator.allocateId(utDef.name), unique, indexType);

                    columnIndex = 0;
                    for (SchemaDef.IndexColumnDef indexColumnDef : indexDef.columns) {
                        Column fkColumn = ut
                                .getColumn(indexColumnDef.columnName);
                        fkIndex.addColumn(new IndexColumn(fkIndex, fkColumn,
                                columnIndex++, !indexColumnDef.descending,
                                indexColumnDef.indexedLength));
                    }
                }
            }
        }
        builder.basicSchemaIsComplete();

        // loop through group tables and add to AIS
        GroupNamer namer = new GroupNamer();
        for (CName group : schemaDef.getGroupMap().keySet()) {
            String groupName = namer.name(group);
            LOG.debug("Group = {}" + groupName);

            builder.createGroup(groupName, group.getSchema(),
                    groupTableName(group));

            List<CName> tablesInGroup = depthFirstSortedUserTables(group);
            for (CName table : tablesInGroup) {
                UserTableDef tableDef = schemaDef.getUserTableMap().get(table);
                IndexDef akibanFK = getAkibanJoin(tableDef);
                if (akibanFK == null) {
                    // No FK: this is a root table so do nothing
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Group Root Table = " + table.getName());
                    }
                    builder.addTableToGroup(groupName, table.getSchema(),
                            table.getName());
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Group Child Table = " + table.getName());
                    }
                    if (akibanFK.referenceTable != null) {
                        String joinName = constructFKJoinName(tableDef,
                                akibanFK);
                        builder.addJoinToGroup(groupName, joinName, 0);
                    }
                }
            }
        }
        if (!schemaDef.getGroupMap().isEmpty())
            builder.groupingIsComplete();

        return builder.akibanInformationSchema();
    }

    private String constructFKJoinName(UserTableDef childTable, IndexDef fkIndex) {
        String ret = (fkIndex.getParentSchema() + "/"
                + fkIndex.getParentTable() + "/"
                + Strings.join(fkIndex.getParentColumns(), ",") + "/"
                + childTable.getCName().getSchema() + "/" + childTable.name
                + "/" + Strings.join(fkIndex.getChildColumns(), ","))
                .toLowerCase();
        return ret.replace(',', '_');
    }

    private Long longValue(final String s) {
        return s == null ? null : Long.valueOf(Long.parseLong(s));
    }

    private String groupTableName(final CName group) {
        return "_akiban_" + group.getName();
    }

}
