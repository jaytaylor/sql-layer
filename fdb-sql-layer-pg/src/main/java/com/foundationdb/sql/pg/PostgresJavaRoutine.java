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

import static com.foundationdb.sql.pg.PostgresJsonStatement.jsonColumnNames;
import static com.foundationdb.sql.pg.PostgresJsonStatement.jsonColumnTypes;
import static com.foundationdb.sql.pg.PostgresJsonStatement.jsonAISColumns;

import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaRoutine;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.ExternalRoutineInvocationException;
import com.foundationdb.server.explain.Explainable;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.io.IOException;

public abstract class PostgresJavaRoutine extends PostgresDMLStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresJavaRoutine: execute shared");

    protected ServerCallInvocation invocation;

    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerCallInvocation invocation,
                                              List<ParameterNode> params, 
                                              int[] paramTypes) {
        Routine routine = invocation.getRoutine();
        List<String> columnNames;
        List<PostgresType> columnTypes;
        List<Column> aisColumns;
        switch (server.getOutputFormat()) {
        case JSON:
        case JSON_WITH_META_DATA:
            columnNames = jsonColumnNames();
            columnTypes = jsonColumnTypes(server.typesTranslator().typeForString());
            aisColumns = jsonAISColumns();
            break;
        default:
            columnTypes = columnTypes(routine);
            if (columnTypes.isEmpty()) {
                columnTypes = null;
                columnNames = null;
                aisColumns = null;
            }
            else {
                columnNames = columnNames(routine);
                aisColumns = Collections.nCopies(columnTypes.size(), null);
            }
            break;
        }
        PostgresType[] parameterTypes;
        if ((params == null) || params.isEmpty())
            parameterTypes = null;
        else
            parameterTypes = parameterTypes(invocation, params.size(), paramTypes);
        switch (routine.getCallingConvention()) {
        case JAVA:
            return PostgresJavaMethod.statement(server, invocation, 
                                                columnNames, columnTypes, aisColumns,
                                                parameterTypes);
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            return PostgresScriptFunctionJavaRoutine.statement(server, invocation, 
                                                               columnNames, columnTypes, aisColumns,
                                                               parameterTypes);
        case SCRIPT_BINDINGS:
        case SCRIPT_BINDINGS_JSON:
            return PostgresScriptBindingsRoutine.statement(server, invocation, 
                                                           columnNames, columnTypes, aisColumns,
                                                           parameterTypes);
        default:
            return null;
        }
    }

    protected PostgresJavaRoutine() {
    }

    protected PostgresJavaRoutine(ServerCallInvocation invocation,
                                  List<String> columnNames, 
                                  List<PostgresType> columnTypes,
                                  List<Column> aisColumns,
                                  PostgresType[] parameterTypes) {
        super.init(null, columnNames, columnTypes, aisColumns, parameterTypes);
        this.invocation = invocation;
    }

    protected abstract ServerJavaRoutine javaRoutine(PostgresQueryContext context, QueryBindings bindings);

    public ServerCallInvocation getInvocation() {
        return invocation;
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.CALL_STMT);
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        Queue<ResultSet> dynamicResultSets = null;
        ServerJavaRoutine call = javaRoutine(context, bindings);
        call.push();
        boolean anyOutput = false, success = false;
        try {
            call.setInputs();
            call.invoke();
            dynamicResultSets = call.getDynamicResultSets();
            if (getColumnTypes() != null) {
                PostgresOutputter<ServerJavaRoutine> outputter;
                switch (server.getOutputFormat()) {
                case JSON:
                case JSON_WITH_META_DATA:
                    outputter = new PostgresJavaRoutineJsonOutputter(context, this,
                                                                     dynamicResultSets);
                    break;
                default:
                    outputter = new PostgresJavaRoutineResultsOutputter(context, this);
                    break;
                }
                outputter.beforeData();
                outputter.output(call);
                nrows++;
                anyOutput = true;
                outputter.afterData();
            }
            if (!dynamicResultSets.isEmpty()) {
                PostgresDynamicResultSetOutputter outputter = 
                    new PostgresDynamicResultSetOutputter(context, this);
                while (!dynamicResultSets.isEmpty()) {
                    ResultSet rs = dynamicResultSets.remove();
                    if (anyOutput) {
                        // Postgres protocol does not allow for
                        // multiple result sets, except as the result
                        // of multiple commands. So pretend that's what we've
                        // got. Even with that, most clients seem to only expose the
                        // last result set.
                        messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
                        messenger.writeString("CALL " + nrows);
                        messenger.sendMessage();
                        nrows = 0;
                    }
                    try {
                        outputter.setMetaData(rs.getMetaData(), context);
                        outputter.sendDescription();
                        while (rs.next()) {
                            outputter.output(rs);
                            nrows++;
                        }
                    }
                    catch (SQLException ex) {
                        throw new ExternalRoutineInvocationException(invocation.getRoutineName(), ex);
                    }
                    finally {
                        try {
                            rs.close();
                        }
                        catch (SQLException ex) {
                        }
                    }
                    anyOutput = true;
                }
            }
            success = true;
        }
        finally {
            call.pop(success);
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("CALL " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    public static List<String> columnNames(Routine routine) {
        List<String> result = new ArrayList<>();
        for (Parameter param : routine.getParameters()) {
            if (param.getDirection() == Parameter.Direction.IN) continue;
            String name = param.getName();
            if (name == null)
                name = String.format("col%d", result.size() + 1);
            result.add(name);
        }
        if (routine.getReturnValue() != null) {
            result.add("return");
        }
        return result;
    }

    public static List<PostgresType> columnTypes(Routine routine) {
        List<PostgresType> result = new ArrayList<>();
        for (Parameter param : routine.getParameters()) {
            if (param.getDirection() == Parameter.Direction.IN) continue;
            result.add(PostgresType.fromAIS(param));
        }
        if (routine.getReturnValue() != null) {
            result.add(PostgresType.fromAIS(routine.getReturnValue()));
        }
        return result;
    }

    public static PostgresType[] parameterTypes(ServerCallInvocation invocation,
                                                int nparams, int[] paramTypes) {
        PostgresType[] result = new PostgresType[nparams];
        for (int i = 0; i < nparams; i++) {
            // See what method argument index this parameter is (first) used for.
            // That will determine its type.
            int usage = invocation.parameterUsage(i);
            if (usage < 0) continue;
            PostgresType pgType = PostgresType.fromAIS(invocation.getRoutineParameter(i));
            if ((paramTypes != null) && (i < paramTypes.length)) {
                // Adjust to match what client proposed.
                PostgresType.TypeOid oid = PostgresType.TypeOid.fromOid(paramTypes[i]);
                if (oid != null) {
                    if (pgType == null)
                        pgType = new PostgresType(oid, (short)-1, -1, null);
                    else
                        pgType = new PostgresType(oid,  (short)-1, -1, 
                                                  pgType.getType());
                }
            }
            result[i] = pgType;
        }
        return result;
    }

    public static Explainable explainable(PostgresServerSession server,
                                          ServerCallInvocation invocation,
                                          List<ParameterNode> params, int[] paramTypes) {
        PostgresJavaRoutine routine = (PostgresJavaRoutine)statement(server, invocation, params, paramTypes);
        PostgresQueryContext context = new PostgresQueryContext(server);
        QueryBindings bindings = context.createBindings();
        return routine.javaRoutine(context, bindings);
    }

}
