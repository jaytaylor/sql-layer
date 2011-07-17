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

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.validation.AISValidations;

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
    
    /**
     * Creates an AISMerger with the starting values. 
     * 
     * @param primaryAIS - where the table will end up
     * @param newTable - UserTable to merge into the primaryAIS
     * @throws Exception
     */
    public AISMerge (AkibanInformationSchema primaryAIS, UserTable newTable) throws Exception {
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
        
        final AISBuilder builder = new AISBuilder(targetAIS);

        // Add the user table to the targetAIS
        addTable (builder, sourceTable); 
        builder.basicSchemaIsComplete();
        // Joins or group table?
        if (sourceTable.getParentJoin() == null) {
            String groupName = groupNames.generateGroupName(sourceTable);
            String groupTableName = groupNames.generateGroupTableName(groupName);
            
            builder.createGroup(groupName, 
                    sourceTable.getName().getSchemaName(), 
                    groupTableName);
            builder.addTableToGroup(groupName, 
                    sourceTable.getName().getSchemaName(), 
                    sourceTable.getName().getTableName());
        } else {
            ; // TODO : currently not yet complete, There is a parent join, perform the join.  
        }
        builder.groupingIsComplete();
        
        builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary();
        builder.akibanInformationSchema().freeze();
        return this;
    }

    private void addTable(AISBuilder builder, final UserTable table) {
        
        // I should use TableSubsetWriter(new AISTarget(targetAIS))
        // but that assumes the UserTable.getAIS() is complete and valid. 
        // i.e. has a group and group table.

        
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
}
