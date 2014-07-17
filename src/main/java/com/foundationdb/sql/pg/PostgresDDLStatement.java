 /**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.aisddl.AISDDL;
import com.foundationdb.sql.aisddl.TableDDL;
import com.foundationdb.sql.parser.*;

import com.foundationdb.qp.operator.QueryBindings;

import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

/** SQL DDL statements. */
public class PostgresDDLStatement extends PostgresBaseStatement
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresDDLStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresDDLStatement: execute shared");

    private DDLStatementNode ddl;
    private String sql;
    private List<String> columnNames = null;
    private List<PostgresType> columnTypes = null;
    PostgresOperatorStatement opstmt;

    public PostgresDDLStatement(DDLStatementNode ddl, String sql) {
        this.sql = sql;
        this.ddl = ddl;
    }

    public PostgresDDLStatement(DDLStatementNode ddl, String sql, PostgresOperatorStatement opstmt) {
        this.sql = sql;
        this.ddl = ddl;
        this.opstmt = opstmt;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        if(opstmt != null) {
            opstmt.finishGenerating(server, sql, ((CreateTableNode) stmt).getQueryExpression(), params, paramTypes);
            columnNames = opstmt.getColumnNames();
            columnTypes = opstmt.getColumnTypes();
        }
        super.finishGenerating(server,sql, stmt, params, paramTypes);
        return this;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            if (params) {
                messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
                messenger.writeShort(0);
                messenger.sendMessage();
            }
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        if(opstmt != null){
            return TransactionMode.IMPLICIT_COMMIT_AND_NEW;
        }
        return TransactionMode.IMPLICIT_COMMIT;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        //if this is a create table node with a query expression use special case
        if(ddl.getNodeType() == NodeTypes.CREATE_TABLE_NODE && ((CreateTableNode)ddl).getQueryExpression() != null){
            try{
                preExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
                String schema = server.getDefaultSchemaName();
                DDLFunctions ddlFunctions = server.getDXL().ddlFunctions();
                Session session = server.getSession();
                List<DataTypeDescriptor> descriptors = new ArrayList<>();
                for(PostgresType columnType: columnTypes){
                    descriptors.add(columnType.getType().dataTypeDescriptor());
                }

                TableDDL.createTable(ddlFunctions, session, schema, (CreateTableNode) ddl, context, descriptors, columnNames, sql, server);
            }
            finally {
                postExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
            }
        }  else {
            try {
                preExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
                AISDDL.execute(ddl, sql, context);
            } finally {
                postExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
            }
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(ddl.statementToString());
            messenger.sendMessage();
        }
        return 0;
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }
}
