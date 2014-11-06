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

import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.TInstance;

import java.net.URL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// AISBuilder can be used to create an AIS. The API is designed to sify the creation of an AIS during a scan
// of a dump. The user need not search the AIS and hold on to AIS objects (Table, Column, etc.). Instead,
// only names from the dump need be supplied. 

public class AISBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(AISBuilder.class);
    // API for creating capturing basic schema information

    // Used when no generator is passed to constructor (i.e. stub or test).
    // Generate temporary, but unique, IDs until the server assigns them.
    private static class SimpleGenerator extends DefaultNameGenerator {
        private int indexID = 1;

        public SimpleGenerator(AkibanInformationSchema ais) {
            super(ais);
            indexID = super.getMaxIndexID();
        }

        @Override
        public int generateIndexID(int rootTableID) {
            return indexID++;
        }
    }

    public AISBuilder() {
        this(new AkibanInformationSchema());
    }

    public AISBuilder(AkibanInformationSchema ais) {
        this(ais, new SimpleGenerator(ais), null);
    }

    public AISBuilder(AkibanInformationSchema ais, NameGenerator nameGenerator,
                      StorageFormatRegistry storageFormatRegistry) {
        LOG.trace("creating builder");
        this.ais = ais;
        this.nameGenerator = nameGenerator;
        this.storageFormatRegistry = storageFormatRegistry;
    }

    public NameGenerator getNameGenerator() {
        return nameGenerator;
    }

    public Sequence sequence(String schemaName, String sequenceName,
                             long start, long increment, long minValue, long maxValue, boolean cycle) {
        LOG.trace("sequence: {}.{} ", schemaName,sequenceName);
        Sequence identityGenerator = Sequence.create(ais, schemaName, sequenceName, start, increment, minValue, maxValue, cycle);
        finishStorageDescription(identityGenerator);
        return identityGenerator;
    }
    
    public Table table(String schemaName, String tableName) {
        LOG.trace("table: " + schemaName + "." + tableName);
        return Table.create(ais, schemaName, tableName, nameGenerator.generateTableID(new TableName(schemaName, tableName)));
    }

    @Deprecated
    public Table tableInitialAutoIncrement(String schemaName,
            String tableName, Long initialAutoIncrementValue) {
        LOG.trace("tableInitialAutoIncrement: " + schemaName + "."
                + tableName + " = " + initialAutoIncrementValue);
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "setting initial autoincrement value", "user table",
                concat(schemaName, tableName));
        table.setInitialAutoIncrementValue(initialAutoIncrementValue);
        return table;
    }

    public void view(String schemaName, String tableName,
                     String definition, Properties definitionProperties,
                     Map<TableName,Collection<String>> tableColumnReferences) {
        LOG.trace("view: " + schemaName + "." + tableName);
        View.create(ais, schemaName, tableName, 
                    definition, definitionProperties, tableColumnReferences);
    }

    public Column column(String schemaName, String tableName, String columnName,
                Integer position, TInstance type, Boolean autoIncrement,
                String defaultValue, String defaultFunction) {
        Columnar table = ais.getColumnar(schemaName, tableName);
        checkFound(table, "creating column", "user table",
                concat(schemaName, tableName));
        return column(table, columnName, position, type,
                      autoIncrement, defaultValue, defaultFunction);
    }

    private Column column(Columnar table, String columnName,
                          Integer position, TInstance type, Boolean autoIncrement,
                          String defaultValue, String defaultFunction) {
        LOG.trace("column: " + table + "." + columnName);
        Column column = Column.create(table, columnName, position, type);
        column.setAutoIncrement(autoIncrement);
        column.setDefaultValue(defaultValue);
        column.setDefaultFunction(defaultFunction);
        column.finishCreating();
        return column;
    }

    public void columnAsIdentity(String schemaName, String tableName, String columnName,
                                 long start, long increment, boolean defaultIdentity) {
        // The merge process will generate the *real* sequence name
        // Note: This will generate the same name twice and is intended for now, see bug1168552 discussion.
        String sequenceName = tableName + "-temp-sequence-1";
        long min = (increment > 0) ? start : Long.MIN_VALUE;
        long max = (increment > 0) ? Long.MAX_VALUE : start;
        sequence(schemaName, sequenceName, start, increment, min, max, false);
        columnAsIdentity(schemaName, tableName, columnName, sequenceName, defaultIdentity);
    }

    public void columnAsIdentity (String schemaName, String tableName, String columnName,
            String sequenceName, Boolean defaultIdentity) {
        LOG.trace("column as identity: " + schemaName + "." + tableName + "." + columnName + ": " + sequenceName);
        Column column = ais.getTable(schemaName, tableName).getColumn(columnName);
        column.setDefaultIdentity(defaultIdentity);
        Sequence identityGenerator = ais.getSequence(new TableName (schemaName, sequenceName));
        column.setIdentityGenerator(identityGenerator);
    }

    public void pk(String schemaName, String tableName) {
        TableName constraintName = nameGenerator.generatePKConstraintName(schemaName, tableName);
        index(schemaName, tableName, Index.PRIMARY, true, true, constraintName);        
    }

    public void pkConstraint(String schemaName, String tableName, TableName constraintName) {
        index(schemaName, tableName, Index.PRIMARY, true, true, constraintName);
    }
    
    public void unique(String schemaName, String tableName, String indexName) {
        TableName constraintName = nameGenerator.generateUniqueConstraintName(schemaName, tableName);
        index(schemaName, tableName, indexName, true, false, constraintName);
    }

    public void uniqueConstraint(String schemaName, String tableName, String indexName, TableName constraintName) {
        index(schemaName, tableName, indexName, true, false, constraintName);
    }
    
    public void index(String schemaName, String tableName, String indexName) {
        index(schemaName, tableName, indexName, false, false, null);
    }

    public void index(String schemaName, String tableName, String indexName, Boolean unique, Boolean isPrimaryKey, TableName constraintName) {
        index(schemaName, tableName, indexName, unique, isPrimaryKey, constraintName, null);
    }

    public void index(String schemaName, String tableName, String indexName, Boolean unique, Boolean isPrimaryKey, TableName constraintName, StorageDescription copyStorage) {
        Table table = ais.getTable(schemaName, tableName);
        int indexID = nameGenerator.generateIndexID(getRooTableID(table));
        LOG.trace("index: " + schemaName + "." + tableName + "." + indexName);
        checkFound(table, "creating index", "table", concat(schemaName, tableName));
        Index index = TableIndex.create(ais, table, indexName, indexID, unique, isPrimaryKey, constraintName);
        finishStorageDescription(index, copyStorage);
    }

    // deprecate?
    public void groupIndex(String groupName, String indexName, Boolean unique, Index.JoinType joinType)
    {
        groupIndex(findFullGroupName(groupName), indexName, unique, joinType);
    }

    public void groupIndex(TableName groupName, String indexName, Boolean unique, Index.JoinType joinType)
    {
        LOG.trace("groupIndex: " + groupName + "." + indexName);
        Group group = ais.getGroup(groupName);
        checkFound(group, "creating group index", "group", groupName);
        setRootIfNeeded(group);
        int indexID = nameGenerator.generateIndexID(getRooTableID(group.getRoot()));
        Index index = GroupIndex.create(ais, group, indexName, indexID, unique, false, joinType);
        finishStorageDescription(index);
    }

    public void indexColumn(String schemaName, String tableName,
            String indexName, String columnName, Integer position,
            Boolean ascending, Integer indexedLength) {
        LOG.trace("indexColumn: " + schemaName + "." + tableName + "."
                + indexName + ":" + columnName);
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "creating index column", "table",
                concat(schemaName, tableName));
        Column column = table.getColumn(columnName);
        checkFound(column, "creating index column", "column",
                concat(schemaName, tableName, columnName));
        Index index = table.getIndex(indexName);
        checkFound(table, "creating index column", "index",
                concat(schemaName, tableName, indexName));
        IndexColumn.create(index, column, position, ascending, indexedLength);
    }

    // deprecate?
    public void groupIndexColumn(String groupName, String indexName, String schemaName, String tableName,
                                 String columnName, Integer position) {
        groupIndexColumn(findFullGroupName(groupName), indexName, schemaName, tableName, columnName, position);
    }

    public void groupIndexColumn(TableName groupName, String indexName, String schemaName, String tableName,
                                 String columnName, Integer position)
    {
        LOG.trace("groupIndexColumn: " + groupName + "." + indexName + ":" + columnName);
        Group group = ais.getGroup(groupName);
        checkFound(group, "creating group index column", "group", groupName);
        Index index = group.getIndex(indexName);
        checkFound(index, "creating group index column", "index", concat(groupName.toString(), indexName));
        Table table = ais.getTable(schemaName, tableName);
        if (!table.getGroup().getName().equals(groupName)) {
            throw new IllegalArgumentException("group name mismatch: " + groupName + " != " + table.getGroup());
        }
        checkFound(table, "creating group index column", "table", concat(schemaName, tableName));
        Column column = table.getColumn(columnName);
        checkFound(column, "creating group index column", "column", concat(schemaName, tableName, columnName));
        IndexColumn.create(index, column, position, true, null);
    }

    public void fullTextIndex(TableName tableName, String indexName)
    {
        LOG.trace("fullTextIndex: " + tableName + "." + indexName);
        Table table = ais.getTable(tableName);
        checkFound(table, "creating full text index", "table", tableName);
        int indexID = nameGenerator.generateIndexID(getRooTableID(table));
        FullTextIndex.create(ais, table, indexName, indexID);
    }

    public void fullTextIndexColumn(TableName indexedTableName, String indexName, 
                                    String schemaName, String tableName, String columnName, Integer position)
    {
        LOG.trace("fullTextIndexColumn: " + indexedTableName + "." + indexName + ":" + columnName);
        Table indexedTable = ais.getTable(indexedTableName);
        checkFound(indexedTable, "creating full text index column", "table", indexedTableName);
        Index index = indexedTable.getFullTextIndex(indexName);
        checkFound(index, "creating full text index column", "index", concat(tableName.toString(), indexName));
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "creating full text index column", "table", concat(schemaName, tableName));
        Column column = table.getColumn(columnName);
        checkFound(column, "creating full text index column", "column", concat(schemaName, tableName, columnName));
        IndexColumn.create(index, column, position, true, null);
    }

    public void joinTables(String joinName, String parentSchemaName,
            String parentTableName, String childSchemaName,
            String childTableName) {
        LOG.trace("joinTables: " + joinName + ": " + childSchemaName + "."
                + childTableName + " -> " + parentSchemaName + "."
                + parentTableName);
        Table child = ais.getTable(childSchemaName, childTableName);
        checkFound(child, "creating join", "child table",
                concat(childSchemaName, childTableName));
        Table parent = ais.getTable(parentSchemaName, parentTableName);
        if (parent == null) {
            TableName parentName = new TableName(parentSchemaName,
                    parentTableName);
            ForwardTableReference forwardTableReference = new ForwardTableReference(
                    joinName, parentName, child);
            forwardReferences.put(joinName, forwardTableReference);
        } else {
            Join.create(ais, joinName, parent, child);
        }
    }

    public void joinColumns(String joinName, String parentSchemaName,
            String parentTableName, String parentColumnName,
            String childSchemaName, String childTableName,
            String childColumnName)

    {
        LOG.trace("joinColumns: " + joinName + ": " + childSchemaName + "."
                + childTableName + "." + childColumnName + " -> "
                + parentSchemaName + "." + parentTableName + "."
                + parentColumnName);
        // Get child info
        Table childTable = ais
                .getTable(childSchemaName, childTableName);
        checkFound(childTable, "creating join column", "child table",
                concat(childSchemaName, childTableName));
        Column childColumn = childTable.getColumn(childColumnName);
        checkFound(childColumn, "creating join column", "child column",
                concat(childSchemaName, childTableName, childColumnName));
        // Handle parent - could be a forward reference
        Table parentTable = ais.getTable(parentSchemaName,
                parentTableName);
        if (parentTable == null) {
            // forward reference
            ForwardTableReference forwardTableReference = forwardReferences
                    .get(joinName);
            forwardTableReference.addColumnReference(parentColumnName,
                    childColumn);
        } else {
            // we've seen the child table
            Column parentColumn = parentTable.getColumn(parentColumnName);
            checkFound(parentColumn, "creating join column", "parent column",
                    concat(parentSchemaName, parentTableName, parentColumnName));
            Join join = ais.getJoin(joinName);
            checkFound(
                    join,
                    "creating join column",
                    "join",
                    concat(parentSchemaName, parentTableName, parentColumnName)
                            + "/"
                            + concat(childSchemaName, childTableName,
                                    childColumnName));
            join.addJoinColumn(parentColumn, childColumn);
        }
    }

    public void routine(String schemaName, String routineName,
                        String language, Routine.CallingConvention callingConvention) {
        LOG.trace("routine: {}.{} ", schemaName, routineName);
        Routine routine = Routine.create(ais, schemaName, routineName,
                                               language, callingConvention);
    }

    public void parameter(String schemaName, String routineName, 
                          String parameterName, Parameter.Direction direction, 
                          TInstance type) {
        LOG.trace("parameter: {} {}", concat(schemaName, routineName), parameterName);
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "creating parameter", "routine", 
                   concat(schemaName, routineName));
        Parameter parameter = Parameter.create(routine, parameterName, direction,
                type);
    }

    public void routineExternalName(String schemaName, String routineName,
                                    String jarSchema, String jarName, 
                                    String className, String methodName) {
        LOG.trace("external name: {} {}", concat(schemaName, routineName), concat(jarName, className, methodName));
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "external name", "routine", 
                   concat(schemaName, routineName));
        SQLJJar sqljJar = null;
        if (jarName != null) {
            sqljJar = ais.getSQLJJar(jarSchema, jarName);
            checkFound(sqljJar, "external name", "SQJ/J jar", 
                       concat(jarSchema, jarName));
        }
        routine.setExternalName(sqljJar, className, methodName);
    }

    public void routineDefinition(String schemaName, String routineName,
                                  String definition) {
        LOG.trace("external name: {} {}", concat(schemaName, routineName), definition);
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "external name", "routine", 
                   concat(schemaName, routineName));
        routine.setDefinition(definition);
    }

    public void routineSQLAllowed(String schemaName, String routineName,
                                  Routine.SQLAllowed sqlAllowed) {
        LOG.trace("SQL allowed: {} {}", concat(schemaName, routineName), sqlAllowed);
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "SQL allowed", "routine", 
                   concat(schemaName, routineName));
        routine.setSQLAllowed(sqlAllowed);
    }

    public void routineDynamicResultSets(String schemaName, String routineName,
                                         int dynamicResultSets) {
        LOG.trace("dynamic result sets: {} {}", concat(schemaName, routineName), dynamicResultSets);
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "dynamic result sets", "routine", 
                   concat(schemaName, routineName));
        routine.setDynamicResultSets(dynamicResultSets);
    }

    public void routineDeterministic(String schemaName, String routineName,
                                     boolean deterministic) {
        LOG.trace("deterministic: {} {}", concat(schemaName, routineName), deterministic);
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "deterministic", "routine", 
                   concat(schemaName, routineName));
        routine.setDeterministic(deterministic);
    }

    public void routineCalledOnNullInput(String schemaName, String routineName,
                                         boolean calledOnNullInput) {
        LOG.trace("called on NULL input: {} {}", concat(schemaName, routineName), calledOnNullInput);
        Routine routine = ais.getRoutine(schemaName, routineName);
        checkFound(routine, "calledOnNullInput", "routine", 
                   concat(schemaName, routineName));
        routine.setCalledOnNullInput(calledOnNullInput);
    }

    public void sqljJar(String schemaName, String jarName, URL url) {
        LOG.trace("SQL/J jar: {}.{} ", schemaName, jarName);
        SQLJJar sqljJar = SQLJJar.create(ais, schemaName, jarName, url);
    }

    public void basicSchemaIsComplete() {
        LOG.trace("basicSchemaIsComplete");
        for (ForwardTableReference forwardTableReference : forwardReferences.values()) {
            Table childTable = forwardTableReference.childTable();
            Table parentTable = ais.getTable(forwardTableReference
                    .parentTableName().getSchemaName(), forwardTableReference
                    .parentTableName().getTableName());
            
            if (parentTable != null){
                Join join = Join.create(ais, forwardTableReference.joinName(),
                        parentTable, childTable);
                for (ForwardColumnReference forwardColumnReference : forwardTableReference
                        .forwardColumnReferences()) {
                    Column childColumn = forwardColumnReference.childColumn();
                    Column parentColumn = parentTable
                            .getColumn(forwardColumnReference.parentColumnName());
                    checkFound(childColumn, "marking basic schema complete",
                            "parent column",
                            forwardColumnReference.parentColumnName());
                    join.addJoinColumn(parentColumn, childColumn);
                }
            }
        }
        forwardReferences.clear();
    }

    // API for describing groups

    public void createGroup(String groupName, String groupSchemaName) {
        createGroup(groupName, groupSchemaName, null);
    }

    public void createGroup(String groupName, String groupSchemaName,
                            StorageDescription copyStorage) {
        LOG.trace("createGroup: {} in {}", groupName, groupSchemaName);
        Group group = Group.create(ais, groupSchemaName, groupName);
        finishStorageDescription(group, copyStorage);
    }

    // deprecate?
    public void deleteGroup(String groupName) {
        deleteGroup(findFullGroupName(groupName));
    }

    public void deleteGroup(TableName groupName) {
        LOG.trace("deleteGroup: " + groupName);
        Group group = ais.getGroup(groupName);
        checkFound(group, "deleting group", "group", groupName);
        boolean groupEmpty = true;
        for (Table table : ais.getTables().values()) {
            if (table.getGroup() == group) {
                groupEmpty = false;
            }
        }
        if (groupEmpty) {
            ais.deleteGroup(group);
        } else {
            throw new GroupNotEmptyException(group);
        }
    }

    // deprecate?
    public void addTableToGroup(String groupName, String schemaName,
            String tableName) {
        addTableToGroup(findFullGroupName(groupName), schemaName, tableName);
    }

    public void addTableToGroup(TableName groupName, String schemaName, String tableName) {
        LOG.trace("addTableToGroup: " + groupName + ": " + schemaName + "."
                + tableName);
        // group
        Group group = ais.getGroup(groupName);
        checkFound(group, "adding table to group", "group", groupName);
        // table
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "adding table to group", "table",
                concat(schemaName, tableName));
        checkGroupAddition(group, table.getGroup(),
                concat(schemaName, tableName));
        setTablesGroup(table, group);
    }

    // addJoinToGroup and removeJoinFromGroup identify a join based on parent
    // and child tables. This is OK for
    // removeJoinFromGroup because of the restrictions on group structure. It
    // DOES NOT WORK for addJoinToGroup,
    // because there could be multiple candidate joins between a pair of tables.

    // deprecate?
    public void addJoinToGroup(String groupName, String joinName, Integer weight) {
        addJoinToGroup(findFullGroupName(groupName), joinName, weight);
    }

    public void addJoinToGroup(TableName groupName, String joinName, Integer weight) {
        LOG.trace("addJoinToGroup: " + groupName + ": " + joinName);
        // join
        Join join = ais.getJoin(joinName);
        checkFound(join, "adding join to group", "join", joinName);
        // group
        Group group = ais.getGroup(groupName);
        checkFound(group, "adding join to group", "group", groupName);
        // parent
        String parentSchemaName = join.getParent().getName().getSchemaName();
        String parentTableName = join.getParent().getName().getTableName();
        Table parent = ais.getTable(parentSchemaName, parentTableName);
        checkFound(parent, "adding join to group", "parent table",
                concat(parentSchemaName, parentTableName));
        checkGroupAddition(group, parent.getGroup(),
                concat(parentSchemaName, parentTableName));
        setTablesGroup(parent, group);
        // child
        String childSchemaName = join.getChild().getName().getSchemaName();
        String childTableName = join.getChild().getName().getTableName();
        Table child = ais.getTable(childSchemaName, childTableName);
        checkFound(child, "adding join to group", "child table",
                concat(childSchemaName, childTableName));
        checkGroupAddition(group, child.getGroup(),
                concat(childSchemaName, childTableName));
        checkCycle(child, group);
        setTablesGroup(child, group);
        join.setGroup(group);
        assert join.getParent() == parent : join;
        checkGroupAddition(group, join.getGroup(), joinName);
    }

    public void removeTableFromGroup(String groupName, String schemaName,
            String tableName) {
        removeTableFromGroup(findFullGroupName(groupName), schemaName, tableName);
    }

    public void removeTableFromGroup(TableName groupName, String schemaName, String tableName) {
        LOG.trace("removeTableFromGroup: " + groupName + ": " + schemaName + "."
                + tableName);
        // This is only valid for a single-table group.
        // group
        Group group = ais.getGroup(groupName);
        checkFound(group, "removing join from group", "group", groupName);
        // table
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "removing join from group", "table table",
                concat(schemaName, tableName));
        checkInGroup(group, table, "removing join from group", "table table");
        if (table.getParentJoin() != null || !table.getChildJoins().isEmpty()) {
            throw new GroupStructureException(
                    "Cannot remove table from a group unless "
                            + "it is the only table in the group, group "
                            + group.getName() + ", table " + table.getName());
        }
        setTablesGroup(table, null);
    }

    // deprecate?
    public void removeJoinFromGroup(String groupName, String joinName) {
        removeJoinFromGroup(findFullGroupName(groupName), joinName);
    }

    public void removeJoinFromGroup(TableName groupName, String joinName) {
        LOG.trace("removeJoinFromGroup: " + groupName + ": " + joinName);
        // join
        Join join = ais.getJoin(joinName);
        checkFound(join, "removing join from group", "join", joinName);
        // group
        Group group = ais.getGroup(groupName);
        checkFound(group, "removing join from group", "group", groupName);
        checkInGroup(group, join, "removing join from group", "child table");
        // parent
        String parentSchemaName = join.getParent().getName().getSchemaName();
        String parentTableName = join.getParent().getName().getTableName();
        Table parent = ais.getTable(parentSchemaName, parentTableName);
        checkFound(parent, "removing join from group", "parent table",
                concat(parentSchemaName, parentTableName));
        checkInGroup(group, parent, "removing join from group", "parent table");
        // child
        String childSchemaName = join.getChild().getName().getSchemaName();
        String childTableName = join.getChild().getName().getTableName();
        Table child = ais.getTable(childSchemaName, childTableName);
        checkFound(child, "removing join from group", "child table",
                concat(childSchemaName, childTableName));
        checkInGroup(group, child, "removing join from group", "child table");
        // Remove the join from the group
        join.setGroup(null);
        // Remove the parent from the group if it isn't involved in any other
        // joins in this group.
        if (parent.getChildJoins().size() == 0
                && parent.getParentJoin() == null) {
            setTablesGroup(parent, null);
        }
        // Same for the child (except we know that parent is null)
        assert child.getParentJoin() == null;
        if (child.getChildJoins().size() == 0) {
            setTablesGroup(child, null);
        }
    }

    // deprecate?
    public void moveTreeToGroup(String schemaName, String tableName,
            String groupName, String joinName) {
        moveTreeToGroup(schemaName, tableName, findFullGroupName(groupName), joinName);
    }

    public void moveTreeToGroup(String schemaName, String tableName, TableName groupName, String joinName) {
        LOG.trace("moveTree: " + schemaName + "." + tableName + " -> "
                + groupName + " via join " + joinName);
        // table
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "moving tree", "table", concat(schemaName, tableName));

        // group
        Group group = ais.getGroup(groupName);
        checkFound(group, "moving tree", "group", groupName);

        // join
        Join join = ais.getJoin(joinName);
        checkFound(join, "moving tree", "join", joinName);

        // Remove table's parent join from its current group (if there is a
        // parent)
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            parentJoin.setGroup(null);
        }

        // Move table to group. Get the children first, because moving the table
        // to another group will cause
        // getChildJoins() to return empty.
        List<Join> children = table.getChildJoins();
        setTablesGroup(table, group);

        // Move the join to the group
        join.setGroup(group);

        moveTree(children, group);
    }

    public void moveTreeToEmptyGroup(String schemaName, String tableName,
            String groupName) {
        moveTreeToEmptyGroup(schemaName, tableName, findFullGroupName(groupName));
    }

    public void moveTreeToEmptyGroup(String schemaName, String tableName, TableName groupName) {
        LOG.trace("moveTree: " + schemaName + "." + tableName
                + " -> empty group " + groupName);
        // table
        Table table = ais.getTable(schemaName, tableName);
        checkFound(table, "moving tree", "table", concat(schemaName, tableName));

        // group
        Group group = ais.getGroup(groupName);
        checkFound(group, "moving tree", "group", groupName);

        // Remove table's parent join from its current group (if there is a
        // parent)
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            parentJoin.setGroup(null);
        }

        // Move table to group. Get the children first (see comment in
        // moveTreeToGroup).
        List<Join> children = table.getChildJoins();
        setTablesGroup(table, group);

        moveTree(children, group);
    }

    public void groupingIsComplete() {
        LOG.trace("groupingIsComplete");
        // Hook up root tables
        for(Group group : ais.getGroups().values()) {
            setRootIfNeeded(group);
            finishStorageDescription(group);
        }
        // Create hidden PKs if needed. Needs group hooked up before it can be called (to generate index id).
        for (Table table : ais.getTables().values()) {
            
            table.endTable(nameGenerator);
            // endTable may have created new index, set its tree name if so
            Index index = table.getPrimaryKeyIncludingInternal().getIndex();
            finishStorageDescription(index);

            // endTable on non-memory tables may have created a new sequence, set its tree name if so
            if (!table.hasMemoryTableFactory()) {
                Column column = index.getKeyColumns().get(0).getColumn();
                if (column.isAkibanPKColumn()) {
                    Sequence sequence = column.getIdentityGenerator();
                    finishStorageDescription (sequence);
                }
            }
        }
    }

    public void finishStorageDescription(HasStorage object) {
        finishStorageDescription(object, null);
    }

    public void finishStorageDescription(HasStorage object, StorageDescription copyStorage) {
        if (copyStorage != null) {
            assert (object.getStorageDescription() == null);
            object.setStorageDescription(copyStorage.cloneForObject(object));
        }
        if (storageFormatRegistry != null) {
            storageFormatRegistry.finishStorageDescription(object, nameGenerator);
        }
    }

    public void clearGroupings() {
        LOG.trace("clear groupings");
        ais.getGroups().clear();
        for (Table table : ais.getTables().values()) {
            setTablesGroup(table, null);
        }
        for (Join join : ais.getJoins().values()) {
            join.setGroup(null);
        }
    }

    public void foreignKey(String referencingSchemaName, String referencingTableName, List<String> referencingColumnNames,
                           String referencedSchemaName, String referencedTableName, List<String> referencedColumnNames,
                           ForeignKey.Action deleteAction, ForeignKey.Action updateAction,
                           boolean deferrable, boolean initiallyDeferred,
                           String name) {
        LOG.trace("foreign key: " + referencingSchemaName + "." + referencingTableName + referencingColumnNames
                  + " references " + referencedSchemaName + "." + referencedTableName + referencedColumnNames);
        Table referencingTable = ais.getTable(referencingSchemaName, referencingTableName);
        checkFound(referencingTable, "creating foreign key", "referencing table", concat(referencingSchemaName, referencingTableName));
        List<Column> referencingColumns = new ArrayList<>(referencingColumnNames.size());
        for (String columnName : referencingColumnNames) {
            Column column = referencingTable.getColumn(columnName);
            checkFound(column, "creating foreign key", "referencing column",
                       concat(referencingSchemaName, referencingTableName, columnName));
            referencingColumns.add(column);
        }
        Table referencedTable = ais.getTable(referencedSchemaName, referencedTableName);
        checkFound(referencedTable, "creating foreign key", "referenced table", concat(referencedSchemaName, referencedTableName));
        List<Column> referencedColumns = new ArrayList<>(referencedColumnNames.size());
        for (String columnName : referencedColumnNames) {
            Column column = referencedTable.getColumn(columnName);
            checkFound(column, "creating foreign key", "referenced column",
                       concat(referencedSchemaName, referencedTableName, columnName));
            referencedColumns.add(column);
        } 
        // Add the (new) referencing index. Also takes care of duplicate fk name.
        // IndexName must be the same as foreign key constraintName
        index(referencingSchemaName, referencingTableName, name);
        for (int i = 0; i < referencingColumnNames.size(); i++) {
            indexColumn(referencingSchemaName, referencingTableName, name,
                        referencingColumnNames.get(i), i, true, null);
        }
        ForeignKey.create(ais, name,
                          referencingTable, referencingColumns,
                          referencedTable, referencedColumns,
                          deleteAction, updateAction,
                          deferrable, initiallyDeferred);
    }

    // API for getting the created AIS

    public AkibanInformationSchema akibanInformationSchema() {
        LOG.trace("getting AIS");
        return ais;
    }

    private Table findRoot(Group group) {
        Table root = null;
        for(Table table : ais.getTables().values()) {
            if((table.getGroup() == group) && table.isRoot()) {
                if(root != null) {
                    return null; // Multiple roots
                }
                root = table;
            }
        }
        return root;
    }

    private void setRootIfNeeded(Group group) {
        if(group.getRoot() == null) {
            group.setRootTable(findRoot(group));
        }
    }

    private void moveTree(List<Join> joins, Group group) {
        LOG.debug("moving tree " + joins + " to group " + group);
        for (Join join : joins) {
            List<Join> children = join.getChild().getChildJoins();
            setTablesGroup(join.getChild(), group);
            join.setGroup(group);
            moveTree(children, group);
        }
    }

    private void checkFound(Object object, String action, String needed, TableName name) {
        checkFound(object, action, needed, name.toString());
    }

    private void checkFound(Object object, String action, String needed,
            String name) {
        if (object == null) {
            throw new NoSuchObjectException(action, needed, name);
        }
    }

    private void checkGroupAddition(Group group, Group existingGroup,
            String name) {
        if (existingGroup != null && existingGroup != group) {
            throw new GroupStructureException(group, existingGroup, name);
        }
    }

    private void checkInGroup(Group group, HasGroup object, String action,
            String objectDescription) {
        if (object.getGroup() != group) {
            throw new NotInGroupException(group, object, action,
                    objectDescription);
        }
    }

    private void checkCycle(Table table, Group group) {
        if (table.getGroup() == group) {
            String exception = table + " is already in " + group
                    + ". Group must be acyclic";
            throw new GroupStructureException(exception);
        }
    }

    private String concat(String... strings) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                buffer.append(".");
            }
            buffer.append(strings[i]);
        }
        return buffer.toString();
    }

    private void setTablesGroup(Table table, Group group) {
        table.setGroup(group);
    }

    private int getRooTableID(Table table) {
        if(table == null) {
            return -1;
        }
        if(table.getGroup() != null && table.getGroup().getRoot() != null) {
            return table.getGroup().getRoot().getTableId();
        }
        return table.getTableId();
    }

    private TableName findFullGroupName(String groupName) {
        Group group = ais.getGroup(groupName);
        checkFound(group, "looking up group without schema", "group", groupName);
        return group.getName();
    }

    // State
    static final class ColumnName {
        private final TableName table;
        private final String columnName;

        public ColumnName(TableName table, String columnName) {
            this.table = table;
            this.columnName = columnName;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result
                    + ((columnName == null) ? 0 : columnName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object object) {
            if (object == this)
                return true;
            if (!(object instanceof ColumnName))
                return false;
            ColumnName other = (ColumnName) object;
            if (this.table == null && other.table != null)
                return false;
            if (!this.table.equals(other.table))
                return false;
            return (this.columnName == null) ? other.columnName == null
                    : this.columnName.equals(other.columnName);
        }
    }

    public final static int MAX_COLUMN_NAME_LENGTH = 64;

    private final AkibanInformationSchema ais;
    private final Map<String, ForwardTableReference> forwardReferences = // join name
                                                                         // ->
                                                                         // ForwardTableReference
        new LinkedHashMap<>();
    private final NameGenerator nameGenerator;
    private final StorageFormatRegistry storageFormatRegistry;

    // Inner classes

    private class ForwardTableReference {
        public ForwardTableReference(String joinName,
                TableName parentTableName, Table childTable) {
            this.joinName = joinName;
            this.parentTableName = parentTableName;
            this.childTable = childTable;
        }

        public String joinName() {
            return joinName;
        }

        public TableName parentTableName() {
            return parentTableName;
        }

        public Table childTable() {
            return childTable;
        }

        public void addColumnReference(String parentColumnName,
                Column childColumn) {
            forwardColumnReferences.add(new ForwardColumnReference(
                    parentColumnName, childColumn));
        }

        public List<ForwardColumnReference> forwardColumnReferences() {
            return forwardColumnReferences;
        }

        private final String joinName;
        private final Table childTable;
        private final TableName parentTableName;
        private final List<ForwardColumnReference> forwardColumnReferences = new ArrayList<>();
    }

    private class ForwardColumnReference {
        public ForwardColumnReference(String parentColumnName,
                Column childColumn) {
            this.parentColumnName = parentColumnName;
            this.childColumn = childColumn;
        }

        public String parentColumnName() {
            return parentColumnName;
        }

        public Column childColumn() {
            return childColumn;
        }

        private final String parentColumnName;
        private final Column childColumn;
    }

    public static class NoSuchObjectException extends RuntimeException {
        public NoSuchObjectException(String action, String needed, String name) {
            super("While " + action + ", could not find " + needed + " " + name);
        }
    }

    public static class GroupStructureException extends RuntimeException {
        public GroupStructureException(Group group, Group existingGroup,
                String name) {
            super(name + " already belongs to group " + existingGroup.getName()
                    + " so it cannot be associated with group "
                    + group.getName());
        }

        public GroupStructureException(String message) {
            super(message);
        }
    }

    public static class GroupNotEmptyException extends RuntimeException {
        public GroupNotEmptyException(Group group) {
            super(
                    "Group "
                            + group.getName()
                            + " cannot be deleted because it contains at least one user table.");
        }
    }

    public class NotInGroupException extends RuntimeException {
        public NotInGroupException(Group group, HasGroup object, String action,
                String objectDescription) {
            super("While " + action + ", found " + objectDescription
                    + " not in " + group + ", but in " + object.getGroup()
                    + " instead.");
        }
    }
}
