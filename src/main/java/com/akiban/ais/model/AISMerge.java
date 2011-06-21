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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.Writer;
import com.akiban.message.ErrorCode;
import com.akiban.server.InvalidOperationException;

public class AISMerge {
    /* state */
    private AkibanInformationSchema targetAIS;
    private UserTable sourceTable;
    private List<AISMergeValidation> checkers;
    private GroupNamer groupNames; 
    
    public AISMerge (AkibanInformationSchema primaryAIS, UserTable newTable) throws Exception {
        targetAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(targetAIS)).save(primaryAIS);
        
        sourceTable = newTable;

        checkers = new LinkedList<AISMergeValidation>();
        
        groupNames = new GroupNamer();
        for (String groupName : targetAIS.getGroups().keySet()) {
            groupNames.name(groupName);
        }
    }
    
    public AkibanInformationSchema getAIS () {
        return targetAIS;
    }
    
    public void addValidation(AISMergeValidation validator) {
        checkers.add(validator);
    }

    public AISMerge validate () throws InvalidOperationException {
        targetAIS.checkIntegrity();
        for (AISMergeValidation validator : checkers) {
            validator.validate (targetAIS, sourceTable);
        }
        return this;
    }
    
    public AISMerge merge() {
        final AISBuilder builder = new AISBuilder(targetAIS);

        // Add the user table to the targetAIS
        addTable (builder, sourceTable); 
        builder.basicSchemaIsComplete();
        // Joins or group table?
        if (sourceTable.getParentJoin() == null) {
            String groupName = groupNames.name(sourceTable.getName().getTableName());
            String groupTableName = groupTableName(groupName);
            
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
        return this;
    }

    private void addTable(AISBuilder builder, UserTable table) {
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

    private String groupTableName(final String group) {
        return "_akiban_" + group;
    }

    private static class GroupNamer {
        private final Set<String> names = new HashSet<String>();

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
    
    public static class NoDuplicateNames implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, UserTable sourceTable)
            throws InvalidOperationException {
            if (targetSchema.getUserTables().containsKey(sourceTable.getName()) ||
                targetSchema.getGroupTables().containsKey(sourceTable.getName())) {
                throw new InvalidOperationException (ErrorCode.DUPLICATE_TABLE,
                        "Merge Schema already contains a user table: %s",
                        sourceTable.getName().toString());
            }
        }
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema) 
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                validate (targetSchema, table);
            }
            for (GroupTable table : validateSchema.getGroupTables().values()) {
                if (targetSchema.getUserTables().containsKey(table.getName()) ||
                    targetSchema.getGroupTables().containsKey(table.getName())) {
                    throw new InvalidOperationException (ErrorCode.DUPLICATE_TABLE, 
                            "Merge Schema already contains a group table: %s.%s",
                            table.getName().getSchemaName(), table.getName().getTableName());
                }
            }
        }
    }
    
    public static class NoAkibanSchemaTables implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, UserTable sourceTable)
        throws InvalidOperationException {
            if (TableName.AKIBAN_INFORMATION_SCHEMA.equals(sourceTable.getName().getSchemaName())) {
                throw new InvalidOperationException (ErrorCode.PROTECTED_TABLE,
                        "Cannot create table `%s` in protected schema `%s`",
                        sourceTable.getName().getTableName(), 
                        sourceTable.getName().getSchemaName());
            }
        }
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                validate(targetSchema, table);
            }
        }
    }
    
    public static class TableColumnSupportedDatatypes implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, UserTable sourceTable)
        throws InvalidOperationException {
            for (Column column : sourceTable.getColumnsIncludingInternal()) {
                if (!targetSchema.isTypeSupported(column.getType().name())) {
                    throw new InvalidOperationException (ErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Table %s column %s uses unsupported type %s", 
                            sourceTable.getName().toString(), 
                            column.getName(), column.getType().name());
                }
            }
        }
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                validate (targetSchema, table);
            }
        }
    }
    
    public static class IndexColumnsSupportedDatatypes implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, UserTable sourceTable)
        throws InvalidOperationException {
            for (Index index : sourceTable.getIndexesIncludingInternal()) {
                for (IndexColumn col : index.getColumns()) {
                    final String typeName = col.getColumn().getType().name();
                    if (!targetSchema.isTypeSupportedAsIndex(typeName)) {
                        throw new InvalidOperationException(
                                ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE,
                                "Table %s index `%s` has unsupported type `%s` from column `%s`",
                                sourceTable.getName().toString(), index.getIndexName().getName(), 
                                typeName, col.getColumn().getName());

                    }
                }
            }
        }

        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                validate (targetSchema, table);
            }
        }
    }
    
    public static class IndexNamesNotNull implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, UserTable sourceTable)
        throws InvalidOperationException {
            for (Index index : sourceTable.getIndexesIncludingInternal()) {
                if (index.getIndexName() == null ||
                    index.getIndexName().getName() == null)
                    throw new InvalidOperationException (ErrorCode.UNSUPPORTED_OPERATION,
                            "Index name for table %s is null",
                            sourceTable.getName().toString());
            }
        }
        
        
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                validate (targetSchema, table);
            }
        }
    }
}
