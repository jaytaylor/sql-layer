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

package com.akiban.sql.aisddl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.ColumnDefinitionNode;
import com.akiban.sql.parser.ConstraintDefinitionNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.DropTableNode;
import com.akiban.sql.parser.FKConstraintDefinitionNode;
import com.akiban.sql.parser.ResultColumn;
import com.akiban.sql.parser.TableElementNode;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId.FormatIds;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;

/** DDL operations on Tables */
public class TableDDL
{
    private final static Logger logger = LoggerFactory.getLogger(TableDDL.class);
    private TableDDL() {
    }

    public static void dropTable (DDLFunctions ddlFunctions,
                                  Session session, 
                                  String defaultSchemaName,
                                  DropTableNode dropTable)
            throws StandardException {
        com.akiban.sql.parser.TableName parserName = dropTable.getObjectName();
        
        String schemaName = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchemaName;
        TableName tableName = TableName.create(schemaName, parserName.getTableName());
        
        try {
            ddlFunctions.dropTable(session, tableName);
        } catch (InvalidOperationException ex) {
            logger.error(ex.getMessage(), ex.getStackTrace());
            throw new StandardException (ex.getMessage());
        }
    }

    public static void createTable(DDLFunctions ddlFunctions,
                                   Session session,
                                   String defaultSchemaName,
                                   CreateTableNode createTable) 
            throws StandardException {
        if (createTable.getQueryExpression() != null)
            throw new StandardException("Cannot CREATE TABLE from SELECT yet.");

        com.akiban.sql.parser.TableName parserName = createTable.getObjectName();
        String schemaName = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchemaName;
        String tableName = parserName.getTableName();
        
        AISBuilder builder = new AISBuilder();
        
        builder.userTable(schemaName, tableName);

        int colpos = 0;
        // first loop through table elements, add the columns
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof ColumnDefinitionNode) {
                addColumn (builder, (ColumnDefinitionNode)tableElement, schemaName, tableName, colpos++);
            }
        }
        // second pass get the constraints (primary, FKs, and other keys)
        // This needs to be done in two passes as the parser may put the 
        // constraint before the column definition. For example:
        // CREATE TABLE t1 (c1 INT PRIMARY KEY) produces such a result. 
        // The Builder complains if you try to do such a thing. 
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof FKConstraintDefinitionNode) {
                FKConstraintDefinitionNode fkdn = (FKConstraintDefinitionNode)tableElement;
                if (fkdn.isGrouping()) {
                    addJoin (builder, fkdn, schemaName, tableName);
                } else {
                    throw new StandardException ("non-grouping Foreign keys not supported");
                }
            }
            else if (tableElement instanceof ConstraintDefinitionNode) {
                addIndex (builder, (ConstraintDefinitionNode)tableElement, schemaName, tableName);
            }
        }
        builder.basicSchemaIsComplete();
        UserTable table = builder.akibanInformationSchema().getUserTable(schemaName, tableName);
        
        try {
            ddlFunctions.createTable(session, table);
        } catch (InvalidOperationException ex) {
            logger.error(ex.getMessage(), ex.getStackTrace());
            throw new StandardException (ex.getMessage());
        }
    }
    
    private static void addColumn (final AISBuilder builder, final ColumnDefinitionNode cdn, 
            final String schemaName, final String tableName, int colpos)
            throws StandardException {
        DataTypeDescriptor type = cdn.getType();
        Long typeParameter1 = null, typeParameter2 = null;
        String typeName = type.getTypeName();
        switch (type.getTypeId().getTypeFormatId()) {
        case FormatIds.INT_TYPE_ID:
            typeName = Types.INT.name();
            break;
        case FormatIds.CHAR_TYPE_ID:
        case FormatIds.VARCHAR_TYPE_ID:
        case FormatIds.BLOB_TYPE_ID:
        case FormatIds.CLOB_TYPE_ID:
            typeParameter1 = (long)type.getMaximumWidth();
            break;
        case FormatIds.DECIMAL_TYPE_ID:
            typeParameter1 = (long)type.getPrecision();
            typeParameter2 = (long)type.getScale();
            break;
        }
        
        try {
        builder.column(schemaName, tableName, 
                cdn.getColumnName(), 
                Integer.valueOf(colpos), 
                typeName, 
                typeParameter1, typeParameter2, 
                type.isNullable(), 
                cdn.isAutoincrementColumn(),
                null, null);
        } catch (InvalidOperationException ex) {
            logger.error(ex.getMessage(), ex.getStackTrace());
            throw new StandardException (ex);
        }
        if (cdn.isAutoincrementColumn()) {
            builder.userTableInitialAutoIncrement(schemaName, tableName, 
                    cdn.getAutoincrementStart());
        }
    }

    private static void addIndex (final AISBuilder builder, final ConstraintDefinitionNode cdn, 
            final String schemaName, final String tableName) 
            throws StandardException {

        NameGenerator namer = new DefaultNameGenerator();
        String constraint = null;
        String indexName = null;
        
        if (cdn.getConstraintType() == ConstraintDefinitionNode.ConstraintType.CHECK) {
            throw new StandardException ("Check constraints not supported (yet)");
        }
        else if (cdn.getConstraintType() == ConstraintDefinitionNode.ConstraintType.PRIMARY_KEY) {
            constraint = Index.PRIMARY_KEY_CONSTRAINT;
        }
        else if (cdn.getConstraintType() == ConstraintDefinitionNode.ConstraintType.UNIQUE) {
            constraint = Index.UNIQUE_KEY_CONSTRAINT;
        }
        indexName = namer.generateIndexName(cdn.getName(), cdn.getColumnList().get(0).getName(), constraint);
        
        try {
            builder.index(schemaName, tableName, indexName, true, constraint);
        } catch (InvalidOperationException ex) {
            logger.error(ex.getMessage(), ex.getStackTrace());
            throw new StandardException (ex);                    
        }
        
        int colPos = 0;
        for (ResultColumn col : cdn.getColumnList()) {
            try {
                builder.indexColumn(schemaName, tableName, indexName, col.getName(), colPos++, true, 0);
            }catch (InvalidOperationException ex) {
                logger.error(ex.getMessage(), ex.getStackTrace());
                throw new StandardException (ex);                        
            }
        }
    }
    
    private static void addJoin(final AISBuilder builder, final FKConstraintDefinitionNode fkdn, 
            final String schemaName, final String tableName) 
    throws StandardException {

        String parentSchemaName = fkdn.getRefTableName().hasSchema() ?
                fkdn.getRefTableName().getSchemaName() : schemaName;
        String parentTableName = fkdn.getRefTableName().getTableName();
        
        try {
            builder.userTable(parentSchemaName, parentTableName);
        } catch (InvalidOperationException ex) {
            // This happens because the userTable() AISInvariants check throws 
            // DUPLICATE_TABLE error. We don't support two joins in the same table. 
            throw new StandardException (ex.getMessage());
        }
        
        String joinName = String.format("%s/%s/%s/%s",
                parentSchemaName, parentTableName, 
                schemaName, tableName);
        
        builder.joinTables(joinName, parentSchemaName, parentTableName, schemaName, tableName);
        
        int colpos = 0;
        for (ResultColumn column : fkdn.getRefResultColumnList()) {
            String columnName = column.getColumnName();
            builder.column(parentSchemaName, parentTableName, columnName, colpos++, "INT", (long)0, (long)0, false, false, null, null);
            builder.joinColumns(joinName, parentSchemaName, parentTableName, columnName, schemaName, tableName, columnName);
        }
    }
}