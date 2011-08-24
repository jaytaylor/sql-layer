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

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.AlterTableNode;

public class AlterTableDDL {
    private AlterTableDDL() {}
    
    public static void alterTable (DDLFunctions ddlFunctions,
                                  Session session, 
                                  String defaultSchemaName,
                                  AlterTableNode alterTable) {
        throw new UnsupportedSQLException (alterTable.statementToString(), alterTable);
    }
}
