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

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.ColumnDefinitionNode;
import com.akiban.sql.parser.ConstraintDefinitionNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.DropTableNode;
import com.akiban.sql.parser.TableElementNode;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId.FormatIds;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.TableName;

/** DDL operations on Tables */
public class TableDDL
{
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
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof ColumnDefinitionNode) {
                ColumnDefinitionNode cdn = (ColumnDefinitionNode)tableElement;
                DataTypeDescriptor type = cdn.getType();
                Long typeParameter1 = null, typeParameter2 = null;
                switch (type.getTypeId().getTypeFormatId()) {
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
                
                builder.column(schemaName, tableName, 
                        cdn.getColumnName(), 
                        Integer.valueOf(colpos++), 
                        type.getTypeName(), 
                        typeParameter1, typeParameter2, 
                        type.isNullable(), 
                        cdn.isAutoincrementColumn(),
                        null, null);
                if (cdn.isAutoincrementColumn()) {
                    builder.userTableInitialAutoIncrement(schemaName, tableName, 
                            cdn.getAutoincrementStart());
                }
            }
            else if (tableElement instanceof ConstraintDefinitionNode) {
                ConstraintDefinitionNode cdn = (ConstraintDefinitionNode)tableElement;
            }
        }
        builder.basicSchemaIsComplete();
        UserTable table = builder.akibanInformationSchema().getUserTable(schemaName, tableName);
        
        try {
            ddlFunctions.createTable(session, table);
        } catch (InvalidOperationException ex) {
            throw new StandardException (ex.getMessage());
        }
    }
}