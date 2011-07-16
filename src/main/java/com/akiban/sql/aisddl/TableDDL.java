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

import java.util.HashSet;
import java.util.Set;

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
import com.akiban.ais.model.AkibanInformationSchema;
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
        builder.setTableIdOffset(1);
        NameGenerator indexNamer = new DefaultNameGenerator();
        
        builder.userTable(schemaName, tableName);

        int colpos = 0;
        // first loop through table elements, add the columns
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
                // TODO: this is a nasty, and hopefully temporary, hack to 
                // work around that the SQL Parser type name (INTEGER) does
                // not match the ais type name (int) for the same type. 
                String typeName = type.getTypeName();
                if (type.getTypeId().getTypeFormatId() == FormatIds.INT_TYPE_ID) {
                    typeName = Types.INT.name();
                }
                
                builder.column(schemaName, tableName, 
                        cdn.getColumnName(), 
                        Integer.valueOf(colpos++), 
                        typeName, 
                        typeParameter1, typeParameter2, 
                        type.isNullable(), 
                        cdn.isAutoincrementColumn(),
                        null, null);
                if (cdn.isAutoincrementColumn()) {
                    builder.userTableInitialAutoIncrement(schemaName, tableName, 
                            cdn.getAutoincrementStart());
                }
            }
        }
        // second pass get the constraints (primary, FKs, and other keys)
        // This needs to be done in two passes as the parser may put the 
        // constraint before the column definition. For example:
        // CREATE TABLE t1 (c1 INT PRIMARY KEY) produces such a result. 
        // The Builder complains if you try to do such a thing. 
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof FKConstraintDefinitionNode) {
                // Foreign keys, check isGrouping() for grouping keys 
                throw new StandardException ("Foreign keys not supported (yet)");
            }
            else if (tableElement instanceof ConstraintDefinitionNode) {
                ConstraintDefinitionNode cdn = (ConstraintDefinitionNode)tableElement;

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
                indexName = indexNamer.generateIndexName(cdn.getName(), cdn.getColumnList().get(0).getName(), constraint);
                
                builder.index(schemaName, tableName, indexName, true, constraint);
                
                int colPos = 0;
                for (ResultColumn col : cdn.getColumnList()) {
                    builder.indexColumn(schemaName, tableName, indexName, col.getName(), colPos++, true, 0);
                }
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

    private static final class IndexNameGenerator {
        private final Set<String> indexNames;

        public IndexNameGenerator() {
            indexNames = new HashSet<String>();
        }
        
        public String generateName(String indexName, String columnName, String constraint) throws StandardException {
            if (constraint.equals(Index.PRIMARY_KEY_CONSTRAINT)) {
                if (indexNames.contains(Index.PRIMARY_KEY_CONSTRAINT)) {
                    throw new StandardException ("Table already has a Primary key, not allowed to define a second one");
                }
                indexNames.add(Index.PRIMARY_KEY_CONSTRAINT);
                return Index.PRIMARY_KEY_CONSTRAINT;
            }
            
            if (indexName != null && !indexNames.contains(indexName)) {
                indexNames.add(indexName);
                return indexName;
            }
            
            String name = columnName;
            for (int suffixNum=2; indexNames.contains(name); ++suffixNum) {
                name = String.format("%s_%d", columnName, suffixNum);
            }
            indexNames.add(name);
            return name;
        }
    }

    private int computeTableIdOffset(AkibanInformationSchema ais) {
        // Use 1 as default offset because the AAM uses tableID 0 as a marker value.
        int offset = 1;
        for(UserTable table : ais.getUserTables().values()) {
            if(!table.getName().getSchemaName().equals("akiban_information_schema")) {
                offset = Math.max(offset, table.getTableId() + 1);
            }
        }
        return offset;
    }

}