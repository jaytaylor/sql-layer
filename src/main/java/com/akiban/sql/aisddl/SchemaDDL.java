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

import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;

import com.akiban.sql.StandardException;

import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.DropSchemaNode;


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
    
    public static void dropSchema (AkibanInformationSchema ais,
                                    String defaultSchemaName,
                                    DropSchemaNode dropSchema)
    throws StandardException
    {
        final String schemaName = dropSchema.getSchemaName();
        // dropSchema.getDropBehavior() == 1
        // This is the default, 1 == RESTRICT, meaning no drop if schema isn't empty
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
        // If you get to this point the schema name isn't used by any user or group table
        // therefore it is a valid schema to drop
        // TODO: Update the AIS to remove the unused schema. 
    }
}
