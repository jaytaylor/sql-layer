/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.sql.aisddl;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchSequenceException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.CreateSequenceNode;
import com.akiban.sql.parser.DropSequenceNode;

public class SequenceDDL {
    private SequenceDDL() { }
    
    public static void createSequence (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    CreateSequenceNode createSequence) {
        final String schemaName = createSequence.getObjectName().getSchemaName() != null ? createSequence.getObjectName().getSchemaName() : defaultSchemaName;
        
        AISBuilder builder = new AISBuilder();
        builder.sequence(schemaName, 
                createSequence.getObjectName().getTableName(), 
                createSequence.getInitialValue(), 
                createSequence.getStepValue(), 
                createSequence.getMinValue(), 
                createSequence.getMaxValue(), 
                createSequence.isAtomic());
        
        final TableName sequenceName = new TableName(schemaName, createSequence.getObjectName().getTableName());
        Sequence sequence = builder.akibanInformationSchema().getSequence(sequenceName);
        ddlFunctions.createSequence(session, sequence);
    }
    
    public static void dropSequence (DDLFunctions ddlFunctions,
                                        Session session,
                                        String defaultSchemaName,
                                        DropSequenceNode dropSequence) {
        final String schemaName = dropSequence.getObjectName().getSchemaName() != null ? dropSequence.getObjectName().getSchemaName() : defaultSchemaName;
        final TableName sequenceName = TableName.create(schemaName, dropSequence.getObjectName().getTableName());
        
        Sequence sequence = ddlFunctions.getAIS(session).getSequence(sequenceName);
        
        if (sequence == null) {
            throw new NoSuchSequenceException (sequenceName);
        }
        ddlFunctions.dropSequence(session, sequence);
    }
}
