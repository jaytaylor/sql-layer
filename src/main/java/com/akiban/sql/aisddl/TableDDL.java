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

import com.akiban.sql.parser.ColumnDefinitionNode;
import com.akiban.sql.parser.ConstraintDefinitionNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.TableElementNode;
import com.akiban.sql.parser.TableName;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.sql.types.TypeId.FormatIds;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;

/** DDL operations on Tables */
public class TableDDL
{
    private TableDDL() {
    }

    public static void createTable(AkibanInformationSchema ais,
                                   String defaultSchemaName,
                                   CreateTableNode createTable) 
            throws StandardException {
        if (createTable.getQueryExpression() != null)
            throw new StandardException("Cannot CREATE TABLE from SELECT yet.");

        AISBuilder ab = new AISBuilder(ais);
        TableName tableName = createTable.getObjectName();
        String schemaName = tableName.getSchemaName();
        if (schemaName == null)
            schemaName = defaultSchemaName;
        UserTable table = UserTable.create(ais, schemaName, 
                                           // TODO: Akiban DB is case sensitive.
                                           tableName.getTableName().toLowerCase(),
                                           // TODO: tableIdGenerator++ from where?
                                           -1);
        int colpos = 0;
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof ColumnDefinitionNode) {
                ColumnDefinitionNode cdn = (ColumnDefinitionNode)tableElement;
                // TODO: Some abstraction needed here.
                DataTypeDescriptor type = cdn.getType();
                Type aisType = ais.getType(type.getTypeName());
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
                Column column = Column.create(table, cdn.getColumnName(), 
                                              colpos++, aisType);
                column.setTypeParameter1(typeParameter1);
                column.setTypeParameter2(typeParameter2);
                column.setNullable(type.isNullable());
                column.setAutoIncrement(cdn.isAutoincrementColumn());
            }
            else if (tableElement instanceof ConstraintDefinitionNode) {
                ConstraintDefinitionNode cdn = (ConstraintDefinitionNode)tableElement;
            }
        }
    }

}