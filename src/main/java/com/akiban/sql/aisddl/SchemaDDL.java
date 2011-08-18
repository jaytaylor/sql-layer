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
import com.akiban.server.service.session.Session;
import com.akiban.sql.StandardException;

import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.sql.parser.StatementType;


public class SchemaDDL {
    private SchemaDDL () {
    }
    
    public static void createSchema (AkibanInformationSchema ais,
                                   String defaultSchemaName,
                                   CreateSchemaNode createSchema)
        throws StandardException 
    {
        final String schemaName = createSchema.getSchemaName();
        
        for (TableName t : ais.getUserTables().keySet()) {
            if (t.getSchemaName().compareToIgnoreCase(schemaName) == 0) {
                throw new StandardException ("Schema " + schemaName + " already exists");
            }
        }
        
        for (TableName t : ais.getGroupTables().keySet()) {
            if (t.getSchemaName().compareToIgnoreCase(schemaName) == 0) {
                throw new StandardException ("Schema " + schemaName + " already exists");
            }
        }
        
        // If you get to this point, the schema name isn't being used by any user or group table
        // therefore is a valid "new" schema. 
        // TODO: update the AIS to store the new schema. 
    }
    
    public static void dropSchema (DDLFunctions ddlFunctions,
            Session session,
            DropSchemaNode dropSchema)
    throws StandardException
    {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        final String schemaName = dropSchema.getSchemaName();

        // 1 == RESTRICT, meaning no drop if the schema isn't empty 
        if (dropSchema.getDropBehavior() == StatementType.DROP_RESTRICT ||
            dropSchema.getDropBehavior() == StatementType.DROP_DEFAULT) {
            for (TableName t : ais.getUserTables().keySet()) {
                if (t.getSchemaName().compareToIgnoreCase(schemaName) == 0) {
                    throw new StandardException ("Schema " + schemaName + " is in use");
                }
            }
            for (TableName t : ais.getGroupTables().keySet()) {
                if (t.getSchemaName().compareToIgnoreCase(schemaName) == 0) {
                    throw new StandardException ("Schema " + schemaName + " is in use");
                }
            }
            // If the schema isn't used by any existing tables, it has effectively 
            // been dropped, so the drop "succeeds".
        } else if (dropSchema.getDropBehavior() == StatementType.DROP_CASCADE) {
            try {
                ddlFunctions.dropSchema(session, schemaName);
            } catch (Exception e) {
                throw new StandardException (e.getMessage());
            }
        }
    }
}
