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

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.Writer;
import com.akiban.message.ErrorCode;
import com.akiban.server.InvalidOperationException;

public class AISMerge {
    /* state */
    private AkibanInformationSchema sourceAIS;
    private AkibanInformationSchema targetAIS;
    private List<AISMergeValidation> checkers;
    
    public AISMerge (AkibanInformationSchema primaryAIS, AkibanInformationSchema secondaryAIS) throws Exception {
        targetAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(targetAIS)).save(primaryAIS);
        
        sourceAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(sourceAIS)).save(secondaryAIS);
        
        checkers = new LinkedList<AISMergeValidation>();
    }
    
    public AkibanInformationSchema getAIS () {
        return targetAIS;
    }
    
    public void addValidation(AISMergeValidation validator) {
        targetAIS.checkIntegrity();
        sourceAIS.checkIntegrity();
        checkers.add(validator);
    }

    public AISMerge validate () throws InvalidOperationException {
        for (AISMergeValidation validator : checkers) {
            validator.validate(targetAIS, sourceAIS);
        }
        return this;
    }
    
    public AISMerge merge() {
        final AISBuilder builder = new AISBuilder(targetAIS);
        
        // loop through user tables and add to AIS
        // user table
        for (UserTable sourceTable : sourceAIS.getUserTables().values()) {
            final String schemaName = sourceTable.getName().getSchemaName();
            final String tableName = sourceTable.getName().getTableName();
 
            builder.userTable(schemaName, tableName);
            UserTable targetTable = targetAIS.getUserTable(schemaName, tableName); 
            targetTable.setEngine(sourceTable.getEngine());
            targetTable.setCharsetAndCollation(sourceTable.getCharsetAndCollation());
            
            // columns
            for (Column column : sourceTable.getColumns()) {
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
            for (TableIndex index : sourceTable.getIndexes()) {
                builder.index(schemaName, tableName, 
                        index.getIndexName().getName(), 
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
        builder.basicSchemaIsComplete();
        
        // loop through group tables and add to AIS
        builder.groupingIsComplete();
        return this;
    }

    public static class NoDuplicateNames implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema) 
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                if (targetSchema.getUserTables().containsKey(table.getName()) ||
                    targetSchema.getGroupTables().containsKey(table.getName())) {
                    throw new InvalidOperationException (ErrorCode.DUPLICATE_TABLE, 
                            "Merge Schema already contains a user table: %s.%s",
                            table.getName().getSchemaName(), table.getName().getTableName());
                    
                }
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
    
    public class NoAkibanSchemaTables implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                final String schemaName = table.getName().getSchemaName();
                final String tableName = table.getName().getTableName();
                if (TableName.AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
                    throw new InvalidOperationException(ErrorCode.PROTECTED_TABLE,
                            "Cannot create table `%s` in protected schema `%s`",
                            tableName, schemaName);
                }
            }
        }
    }
    
    public class TableColumnSupportedDatatypes implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) {
                final String schemaName = table.getName().getSchemaName();
                final String tableName = table.getName().getTableName();
                for (Column column : table.getColumnsIncludingInternal()) {
                    final String typeName = column.getType().name();
                    if (!targetSchema.isTypeSupported(typeName)) {
                        throw new InvalidOperationException(
                                ErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Table `%s`.`%s` column `%s` is unsupported type %s",
                                schemaName, tableName, column.getName(), typeName);
                    }
                }
            }
        }
    }
    
    public class IndexColumnsSupportedDatatypes implements AISMergeValidation {
        public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema)
        throws InvalidOperationException {
            for (UserTable table : validateSchema.getUserTables().values()) { 
                final String schemaName = table.getName().getSchemaName();
                final String tableName = table.getName().getTableName();
                for (Index index : table.getIndexesIncludingInternal()) {
                    for (IndexColumn col : index.getColumns()) {
                        final String typeName = col.getColumn().getType().name();
                        if (!targetSchema.isTypeSupportedAsIndex(typeName)) {
                            throw new InvalidOperationException(
                                    ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE,
                                    "Table `%s`.`%s` index `%s` has unsupported type `%s` from column `%s`",
                                    schemaName, tableName, index.getIndexName().getName(), 
                                    typeName, col.getColumn().getName());
                        }
                    }
                }
            }
        }
    }
}
