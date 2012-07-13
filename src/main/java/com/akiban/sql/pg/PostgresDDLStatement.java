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

package com.akiban.sql.pg;

import com.akiban.qp.util.OperatorBasedTableCopier;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import com.akiban.sql.aisddl.*;

import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.CreateIndexNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.CreateViewNode;
import com.akiban.sql.parser.DropIndexNode;
import com.akiban.sql.parser.DropTableNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.DropViewNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.RenameNode;

import com.akiban.ais.model.AkibanInformationSchema;

import java.io.IOException;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.*;

/** SQL DDL statements. */
public class PostgresDDLStatement implements PostgresStatement
{
    private DDLStatementNode ddl;

    public PostgresDDLStatement(DDLStatementNode ddl) {
        this.ddl = ddl;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows, boolean usePVals) throws IOException {
        PostgresServerSession server = context.getServer();
        AkibanInformationSchema ais = server.getAIS();
        String schema = server.getDefaultSchemaName();
        DDLFunctions ddlFunctions = server.getDXL().ddlFunctions();
        Session session = server.getSession();
        lock(session);
        try {
            switch (ddl.getNodeType()) {
            case NodeTypes.CREATE_SCHEMA_NODE:
                SchemaDDL.createSchema(ais, schema, (CreateSchemaNode)ddl);
                break;
            case NodeTypes.DROP_SCHEMA_NODE:
                SchemaDDL.dropSchema(ddlFunctions, session, (DropSchemaNode)ddl);
                break;
            case NodeTypes.CREATE_TABLE_NODE:
                TableDDL.createTable(ddlFunctions, session, schema, (CreateTableNode)ddl);
                break;
            case NodeTypes.DROP_TABLE_NODE:
                TableDDL.dropTable(ddlFunctions, session, schema, (DropTableNode)ddl);
                break;
            case NodeTypes.CREATE_VIEW_NODE:
                ViewDDL.createView(ddlFunctions, session, schema, (CreateViewNode)ddl,
                                   server.getBinderContext());
                break;
            case NodeTypes.DROP_VIEW_NODE:
                ViewDDL.dropView(ddlFunctions, session, schema, (DropViewNode)ddl,
                                 server.getBinderContext());
                break;
            case NodeTypes.CREATE_INDEX_NODE:
                IndexDDL.createIndex(ddlFunctions, session, schema, (CreateIndexNode)ddl);
                break;
            case NodeTypes.DROP_INDEX_NODE:
                IndexDDL.dropIndex(ddlFunctions, session, schema, (DropIndexNode)ddl);
                break;
            case NodeTypes.ALTER_TABLE_NODE:
            {
                OperatorBasedTableCopier copier = new OperatorBasedTableCopier(
                        server.getStore().getConfig(),
                        server.getTreeService(),
                        session,
                        server.getStore().getUnderlyingStore()
                );
                AlterTableDDL.alterTable(ddlFunctions, session, copier, schema, (AlterTableNode)ddl);
                break;
            }
            case NodeTypes.RENAME_NODE:
                if (((RenameNode)ddl).getRenameType() == RenameNode.RenameType.INDEX) {
                    IndexDDL.renameIndex(ddlFunctions, session, schema, (RenameNode)ddl);
                } else if (((RenameNode)ddl).getRenameType() == RenameNode.RenameType.TABLE) {
                    TableDDL.renameTable(ddlFunctions, session, schema, (RenameNode)ddl);
                }
            case NodeTypes.REVOKE_NODE:
            default:
                throw new UnsupportedSQLException (ddl.statementToString(), ddl);
            }
        } finally {
            unlock(session);
        }
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(ddl.statementToString());
            messenger.sendMessage();
        }
        return 0;
    }

    private void lock(Session session)
    {
        DXLReadWriteLockHook.only().hookFunctionIn(session, UNSPECIFIED_DDL_WRITE);
    }

    private void unlock(Session session)
    {
        DXLReadWriteLockHook.only().hookFunctionFinally(session, UNSPECIFIED_DDL_WRITE, null);
    }
}
