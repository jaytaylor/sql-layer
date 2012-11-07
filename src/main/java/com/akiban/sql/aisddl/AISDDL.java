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

import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.CreateAliasNode;
import com.akiban.sql.parser.CreateIndexNode;
import com.akiban.sql.parser.CreateSequenceNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.CreateViewNode;
import com.akiban.sql.parser.DropAliasNode;
import com.akiban.sql.parser.DropGroupNode;
import com.akiban.sql.parser.DropIndexNode;
import com.akiban.sql.parser.DropSequenceNode;
import com.akiban.sql.parser.DropTableNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.DropViewNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.RenameNode;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerSession;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AISDDL
{
    private static final Logger logger = LoggerFactory.getLogger(AISDDL.class);

    private AISDDL() {}
    
    public static void execute(DDLStatementNode ddl, String sql, 
                               ServerQueryContext<?> context) {
        ServerSession server = context.getServer();
        AkibanInformationSchema ais = server.getAIS();
        String schema = server.getDefaultSchemaName();
        logger.info("DDL in {}: {}", schema, sql);
        DDLFunctions ddlFunctions = server.getDXL().ddlFunctions();
        Session session = server.getSession();
        switch (ddl.getNodeType()) {
        case NodeTypes.CREATE_SCHEMA_NODE:
            SchemaDDL.createSchema(ais, schema, (CreateSchemaNode)ddl, context);
            return;
        case NodeTypes.DROP_SCHEMA_NODE:
            SchemaDDL.dropSchema(ddlFunctions, session, (DropSchemaNode)ddl, context);
            return;
        case NodeTypes.CREATE_TABLE_NODE:
            TableDDL.createTable(ddlFunctions, session, schema, (CreateTableNode)ddl, context);
            return;
        case NodeTypes.DROP_TABLE_NODE:
            TableDDL.dropTable(ddlFunctions, session, schema, (DropTableNode)ddl, context);
            return;
        case NodeTypes.DROP_GROUP_NODE:
            TableDDL.dropGroup(ddlFunctions, session, schema, (DropGroupNode)ddl, context);
            return;
        case NodeTypes.CREATE_VIEW_NODE:
            ViewDDL.createView(ddlFunctions, session, schema, (CreateViewNode)ddl,
                               server.getBinderContext(), context);
            return;
        case NodeTypes.DROP_VIEW_NODE:
            ViewDDL.dropView(ddlFunctions, session, schema, (DropViewNode)ddl,
                             server.getBinderContext(), context);
            return;
        case NodeTypes.CREATE_INDEX_NODE:
            IndexDDL.createIndex(ddlFunctions, session, schema, (CreateIndexNode)ddl);
            return;
        case NodeTypes.DROP_INDEX_NODE:
            IndexDDL.dropIndex(ddlFunctions, session, schema, (DropIndexNode)ddl, context);
            return;
        case NodeTypes.ALTER_TABLE_NODE:
            AlterTableDDL.alterTable(ddlFunctions, server.getDXL().dmlFunctions(), session, schema, (AlterTableNode)ddl, context);
            return;
        case NodeTypes.RENAME_NODE:
            switch (((RenameNode)ddl).getRenameType()) {
            case INDEX:
                IndexDDL.renameIndex(ddlFunctions, session, schema, (RenameNode)ddl);
                return;
            case TABLE:
                TableDDL.renameTable(ddlFunctions, session, schema, (RenameNode)ddl);
                return;
            }
            break;
        case NodeTypes.CREATE_SEQUENCE_NODE:
            SequenceDDL.createSequence(ddlFunctions, session, schema, (CreateSequenceNode)ddl);
            return;
        case NodeTypes.DROP_SEQUENCE_NODE:
            SequenceDDL.dropSequence(ddlFunctions, session, schema, (DropSequenceNode)ddl, context);
            return;
        case NodeTypes.CREATE_ALIAS_NODE:
            switch (((CreateAliasNode)ddl).getAliasType()) {
            case PROCEDURE:
            case FUNCTION:
                RoutineDDL.createRoutine(ddlFunctions, server.getRoutineLoader(), session, schema, (CreateAliasNode)ddl);
                return;
            }
            break;
        case NodeTypes.DROP_ALIAS_NODE:
            switch (((DropAliasNode)ddl).getAliasType()) {
            case PROCEDURE:
            case FUNCTION:
                RoutineDDL.dropRoutine(ddlFunctions, server.getRoutineLoader(), session, schema, (DropAliasNode)ddl, context);
                return;
            }
            break;
        }
        throw new UnsupportedSQLException(ddl.statementToString(), ddl);
    }
}
