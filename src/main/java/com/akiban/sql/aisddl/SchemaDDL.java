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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.DropSchemaNotAllowedException;
import com.akiban.server.error.DuplicateSchemaException;
import com.akiban.server.service.session.Session;

import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.sql.parser.StatementType;


public class SchemaDDL {
    private SchemaDDL () {
    }
    
    public static void createSchema (AkibanInformationSchema ais,
                                   String defaultSchemaName,
                                   CreateSchemaNode createSchema)
    {
        final String schemaName = createSchema.getSchemaName();
        
        if (checkSchema (ais, schemaName)) {
            throw new DuplicateSchemaException (schemaName);
        }
        
        // If you get to this point, the schema name isn't being used by any user or group table
        // therefore is a valid "new" schema. 
        // TODO: update the AIS to store the new schema. 
    }
    
    public static void dropSchema (DDLFunctions ddlFunctions,
            Session session,
            DropSchemaNode dropSchema)
    {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        final String schemaName = dropSchema.getSchemaName();

        // 1 == RESTRICT, meaning no drop if the schema isn't empty 
        if (dropSchema.getDropBehavior() == StatementType.DROP_RESTRICT ||
            dropSchema.getDropBehavior() == StatementType.DROP_DEFAULT) {
            if (checkSchema (ais, schemaName)) {
                throw new DropSchemaNotAllowedException (schemaName);
            }
            // If the schema isn't used by any existing tables, it has effectively 
            // been dropped, so the drop "succeeds".
        } else if (dropSchema.getDropBehavior() == StatementType.DROP_CASCADE) {
            ddlFunctions.dropSchema(session, schemaName);
        }
    }
    
    /**
     * Check if a schema exists, by checking all the user/group tables names if the schema
     * is used for any of them..
     */
    public static boolean checkSchema (AkibanInformationSchema ais, String schemaName) {
        for (TableName t : ais.getUserTables().keySet()) {
            if (t.getSchemaName().compareToIgnoreCase(schemaName) == 0) {
                return true;
            }
        }
        for (TableName t : ais.getGroupTables().keySet()) {
            if (t.getSchemaName().compareToIgnoreCase(schemaName) == 0) {
                return true;
            }
        }
        return false;
    }
}
