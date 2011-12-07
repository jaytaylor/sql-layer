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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.server.error.JoinToMultipleParentsException;
import com.akiban.server.error.JoinToUnknownTableException;
import com.akiban.server.error.JoinToWrongColumnsException;

/**
 * AISMerge is designed to merge a single UserTable definition into an existing AIS. The merge process 
 * does not assume that UserTable.getAIS() returns a validated and complete 
 * AkibanInformationSchema object. 
 * 
 * AISMerge makes a copy of the primaryAIS (from the constructor) before performing the merge process. 
 * The final results is this copies AIS, plus new table, with the full AISValidations suite run, and 
 * frozen. If you pass a frozen AIS into the merge, the copy process unfreeze the copy.
 * 
 * @See UserTable
 * @See AkibanInformationSchema
 */
public class AISMerge {
    /* state */
    private AkibanInformationSchema targetAIS;
    private UserTable sourceTable;
    private NameGenerator groupNames; 
    private static final Logger LOG = LoggerFactory.getLogger(AISMerge.class);

    /**
     * Creates an AISMerger with the starting values. 
     * 
     * @param primaryAIS - where the table will end up
     * @param newTable - UserTable to merge into the primaryAIS
     * @throws Exception
     */
    public AISMerge (AkibanInformationSchema primaryAIS, UserTable newTable) {
        targetAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(targetAIS)).save(primaryAIS);
        
        sourceTable = newTable;
       
        groupNames = new DefaultNameGenerator().setDefaultGroupNames(targetAIS.getGroups().keySet());
    }
    
    /**
     * Returns the final, updated AkibanInformationSchema. This AIS has been fully 
     * validated and is frozen (no more changes), hence ready for update into the
     * server. 
     * @return - the primaryAIS, after merge() with the UserTable added.
     */
    public AkibanInformationSchema getAIS () {
        return targetAIS;
    }
    
    public AISMerge merge() {
        // I should use TableSubsetWriter(new AISTarget(targetAIS))
        // but that assumes the UserTable.getAIS() is complete and valid. 
        // i.e. has a group and group table, joins are accurate, etc. 
        // this may not be true 
        // Also the tableIDs need to be assigned correctly, which 
        // TableSubsetWriter doesn't do. 
        LOG.info(String.format("Merging table %s into targetAIS", sourceTable.getName().toString()));

        final AISBuilder builder = new AISBuilder(targetAIS);
        builder.setTableIdOffset(computeTableIdOffset(targetAIS));

        if (sourceTable.getParentJoin() != null) {
            String parentSchemaName = sourceTable.getParentJoin().getParent().getName().getSchemaName();
            String parentTableName = sourceTable.getParentJoin().getParent().getName().getTableName(); 
            UserTable parentTable = targetAIS.getUserTable(parentSchemaName, parentTableName);
            if (parentTable == null) {
                throw new JoinToUnknownTableException (sourceTable.getName(), new TableName(parentSchemaName, parentTableName));
            }
            builder.setIndexIdOffset(computeIndexIDOffset(targetAIS, parentTable.getGroup().getName()));
        }

        // Add the user table to the targetAIS
        addTable (builder, sourceTable); 

        // Joins or group table?
        if (sourceTable.getParentJoin() == null) {
            LOG.debug("Table is root or lone table");
            String groupName = groupNames.generateGroupName(sourceTable);
            String groupTableName = groupNames.generateGroupTableName(groupName);
            builder.basicSchemaIsComplete();            
            builder.createGroup(groupName, 
                    sourceTable.getName().getSchemaName(), 
                    groupTableName);
            builder.addTableToGroup(groupName, 
                    sourceTable.getName().getSchemaName(), 
                    sourceTable.getName().getTableName());
        } else {
            // Normally there should be only one candidate parent join.
            // But since the AIS supports multiples, so does the merge.
            // This gets flagged in JoinToOneParent validation. 
            for (Join join : sourceTable.getCandidateParentJoins()) {
                addJoin (builder, join);
            }
        }
        builder.groupingIsComplete();
        
        builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary();
        builder.akibanInformationSchema().freeze();
        return this;
    }

    private void addTable(AISBuilder builder, final UserTable table) {
        
        // I should use TableSubsetWriter(new AISTarget(targetAIS))
        // but that assumes the UserTable.getAIS() is complete and valid. 
        // i.e. has a group and group table, and the joins point to a valid table
        // which, given the use of AISMerge, is not true. 
        
        
        final String schemaName = table.getName().getSchemaName();
        final String tableName = table.getName().getTableName();
        

        builder.userTable(schemaName, tableName);
        UserTable targetTable = targetAIS.getUserTable(schemaName, tableName); 
        targetTable.setEngine(table.getEngine());
        targetTable.setCharsetAndCollation(table.getCharsetAndCollation());
        
        // columns
        for (Column column : table.getColumns()) {
            builder.column(schemaName, tableName, 
                    column.getName(), column.getPosition(), 
                    column.getType().name(), 
                    column.getTypeParameter1(), column.getTypeParameter2(), 
                    column.getNullable(), 
                    column.getInitialAutoIncrementValue() != null, 
                    column.getCharsetAndCollation().charset(), 
                    column.getCharsetAndCollation().collation());
            // if an auto-increment column, set the starting value. 
            if (column.getInitialAutoIncrementValue() != null) {
                targetTable.getColumn(column.getPosition()).setInitialAutoIncrementValue(column.getInitialAutoIncrementValue());
            }
        }
        
        // indexes/constraints
        for (TableIndex index : table.getIndexes()) {
            IndexName indexName = index.getIndexName();
            
            builder.index(schemaName, tableName, 
                    indexName.getName(), 
                    index.isUnique(), 
                    index.getConstraint());
            for (IndexColumn col : index.getColumns()) {
                    builder.indexColumn(schemaName, tableName, index.getIndexName().getName(),
                        col.getColumn().getName(), 
                        col.getPosition(), 
                        col.isAscending(), 
                        col.getIndexedLength());
            }
        }
    }

    private void addJoin (AISBuilder builder, Join join) {
        String parentSchemaName = join.getParent().getName().getSchemaName();
        String parentTableName = join.getParent().getName().getTableName();
        UserTable parentTable = targetAIS.getUserTable(parentSchemaName, parentTableName);
        if (parentTable == null) {
            throw new JoinToUnknownTableException(sourceTable.getName(), new TableName(parentSchemaName, parentTableName));
         }
        LOG.debug(String.format("Table is child of table %s", parentTable.getName().toString()));
        String joinName = groupNames.generateJoinName(parentTable.getName(),
                                                      sourceTable.getName(),
                                                      join.getJoinColumns());
        builder.joinTables(joinName,
                parentSchemaName,
                parentTableName,
                sourceTable.getName().getSchemaName(), 
                sourceTable.getName().getTableName());

        for (JoinColumn joinColumn : join.getJoinColumns()) {
            try {
            builder.joinColumns(joinName, 
                    parentSchemaName, 
                    parentTableName, 
                    joinColumn.getParent().getName(),
                    sourceTable.getName().getSchemaName(), 
                    sourceTable.getName().getTableName(), 
                    joinColumn.getChild().getName());
            } catch (AISBuilder.NoSuchObjectException ex) {
                throw new JoinToWrongColumnsException (
                        sourceTable.getName(), joinColumn.getChild().getName(),
                        new TableName(parentSchemaName, parentTableName),
                        joinColumn.getParent().getName());
            }
        }
        builder.basicSchemaIsComplete();
        
        try {
            builder.addJoinToGroup(parentTable.getGroup().getName(), joinName, 0);
        } catch (AISBuilder.GroupStructureException ex) {
            throw new JoinToMultipleParentsException(join.getChild().getName());
        }
    }

    // FOR DEBUGGING
/*
    private void dumpGroupStructure(String label, AkibanInformationSchema ais)
    {
        for (Group group : ais.getGroups().values()) {
            if (!group.getGroupTable().getRoot().getName().getSchemaName().equals("akiban_information_schema")) {
                System.out.println(String.format("%s: Group %s", label, group.getName()));
                System.out.println("    tables:");
                for (UserTable userTable : ais.getUserTables().values()) {
                    if (userTable.getGroup() == group) {
                        System.out.println(String.format("        %s -> %s", userTable, userTable.parentTable()));
                    }
                }
                System.out.println("    joins:");
                for (Join join : ais.getJoins().values()) {
                    if (join.getGroup() == group) {
                        System.out.println(String.format("        %s -> %s", join.getChild(), join.getParent()));
                    }
                }
            }
        }
    }
*/

    private int computeTableIdOffset(AkibanInformationSchema ais) {
        // Use 1 as default offset because the AAM uses tableID 0 as a marker value.
        int offset = 1;
        for(UserTable table : ais.getUserTables().values()) {
            if(!table.getName().getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA)) {
                offset = Math.max(offset, table.getTableId() + 1);
            }
        }
        for (GroupTable table : ais.getGroupTables().values()) {
            if (!table.getName().getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA)) {
                offset = Math.max(offset, table.getTableId() + 1);
            }
        }
        return offset;
    }

    private int computeIndexIDOffset (AkibanInformationSchema ais, String groupName) {
        int offset = 1;
        Group group = ais.getGroup(groupName);
        for(UserTable table : ais.getUserTables().values()) {
            if(table.getGroup().equals(group)) {
                for(Index index : table.getIndexesIncludingInternal()) {
                    offset = Math.max(offset, index.getIndexId() + 1);
                }
            }
        }
        for (GroupIndex index : group.getIndexes()) {
            offset = Math.max(offset, index.getIndexId() + 1); 
        }
        return offset;
    }
}
