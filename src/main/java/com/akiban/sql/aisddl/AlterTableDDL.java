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

import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.AlterTableNode;

import java.util.Collection;
import java.util.Collections;

public class AlterTableDDL {
    private AlterTableDDL() {}
    
    public static void alterTable(DDLFunctions ddlFunctions,
                                  Session session, 
                                  String defaultSchemaName,
                                  AlterTableNode alterTable) {

        com.akiban.sql.parser.TableName sqlName = alterTable.getObjectName();
        String schemaName = sqlName.hasSchema() ? sqlName.getSchemaName() : defaultSchemaName;
        TableName tableName = TableName.create(schemaName, sqlName.getTableName());
        if (ddlFunctions.getAIS(session).getUserTable(tableName) == null) {
            throw new NoSuchTableException(tableName.getSchemaName(), 
                                           tableName.getTableName());
        }

        if (alterTable.isUpdateStatistics()) {
            analyze(ddlFunctions, tableName,
                    alterTable.isUpdateStatisticsAll() ? null : 
                    Collections.singletonList(alterTable.getIndexNameForUpdateStatistics()));
            return;
        }
        throw new UnsupportedSQLException (alterTable.statementToString(), alterTable);
    }

    protected static void analyze(DDLFunctions ddlFunctions, 
                                  TableName tableName, Collection<String> indexNames) {
        
    }
}
