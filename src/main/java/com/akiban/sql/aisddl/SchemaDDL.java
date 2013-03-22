
package com.akiban.sql.aisddl;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.DropSchemaNotAllowedException;
import com.akiban.server.error.DuplicateSchemaException;
import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.service.session.Session;

import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.sql.parser.ExistenceCheck;
import com.akiban.sql.parser.StatementType;
import com.akiban.qp.operator.QueryContext;

public class SchemaDDL {
    private SchemaDDL () {
    }
    
    public static void createSchema (AkibanInformationSchema ais,
                                   String defaultSchemaName,
                                   CreateSchemaNode createSchema,
                                   QueryContext context)
    {
        final String schemaName = createSchema.getSchemaName();
        ExistenceCheck condition = createSchema.getExistenceCheck();
        
        if (ais.getSchema(schemaName) != null)
            switch(condition)
            {
                case IF_NOT_EXISTS:
                    // schema already exists. does nothing
                    if (context != null)
                        context.warnClient(new DuplicateSchemaException(schemaName));
                    return;
                case NO_CONDITION:
                    throw new DuplicateSchemaException (schemaName);
                default:
                    throw new IllegalStateException("Unexpected condition in CREATE SCHEMA: " + condition);
            }

        // If you get to this point, the schema name isn't being used by any user or group table
        // therefore is a valid "new" schema. 
        // TODO: update the AIS to store the new schema. 
    }
    
    public static void dropSchema (DDLFunctions ddlFunctions,
            Session session,
            DropSchemaNode dropSchema,
            QueryContext context)
    {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        final String schemaName = dropSchema.getSchemaName();
        ExistenceCheck condition = dropSchema.getExistenceCheck();
        
        if (ais.getSchema(schemaName) != null)
        {
            // 1 == RESTRICT, meaning no drop if the schema isn't empty 
            if (dropSchema.getDropBehavior() == StatementType.DROP_RESTRICT ||
                    dropSchema.getDropBehavior() == StatementType.DROP_DEFAULT)
                throw new DropSchemaNotAllowedException (schemaName);
            // If the schema isn't used by any existing tables, it has effectively 
            // been dropped, so the drop "succeeds".
            else if (dropSchema.getDropBehavior() == StatementType.DROP_CASCADE) 
                ddlFunctions.dropSchema(session, schemaName);       
        }
        else
            switch(condition)
            {
                case IF_EXISTS:
                    // schema doesn't exists. does nothing
                    if (context != null)
                        context.warnClient(new NoSuchSchemaException(schemaName));
                    return;
                case NO_CONDITION:
                    throw new NoSuchSchemaException(schemaName);
                default:
                    throw new UnsupportedOperationException("Unexpected condition in DROP SCHEMA: " + condition);
            }
        
    }
    
}
