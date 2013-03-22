package com.akiban.sql.aisddl;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchSequenceException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.CreateSequenceNode;
import com.akiban.sql.parser.DropSequenceNode;
import com.akiban.sql.parser.ExistenceCheck;
import com.akiban.qp.operator.QueryContext;

public class SequenceDDL {
    private SequenceDDL() { }
    
    public static void createSequence (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    CreateSequenceNode createSequence) {
        
        final TableName sequenceName = DDLHelper.convertName(defaultSchemaName, createSequence.getObjectName());
                
        AISBuilder builder = new AISBuilder();
        builder.sequence(sequenceName.getSchemaName(), 
                sequenceName.getTableName(), 
                createSequence.getInitialValue(), 
                createSequence.getStepValue(), 
                createSequence.getMinValue(), 
                createSequence.getMaxValue(), 
                createSequence.isCycle());
        
        Sequence sequence = builder.akibanInformationSchema().getSequence(sequenceName);
        ddlFunctions.createSequence(session, sequence);
    }
    
    public static void dropSequence (DDLFunctions ddlFunctions,
                                        Session session,
                                        String defaultSchemaName,
                                        DropSequenceNode dropSequence,
                                        QueryContext context) {
        final TableName sequenceName = DDLHelper.convertName(defaultSchemaName, dropSequence.getObjectName());
        final ExistenceCheck existenceCheck = dropSequence.getExistenceCheck();

        Sequence sequence = ddlFunctions.getAIS(session).getSequence(sequenceName);
        
        if (sequence == null) {
            if (existenceCheck == ExistenceCheck.IF_EXISTS) {
                if (context != null) {
                    context.warnClient(new NoSuchSequenceException(sequenceName));
                }
                return;
            } 
            throw new NoSuchSequenceException (sequenceName);
        } else {
            ddlFunctions.dropSequence(session, sequenceName);
        }
    }
}
