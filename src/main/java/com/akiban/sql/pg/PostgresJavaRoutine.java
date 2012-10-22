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

import static com.akiban.sql.pg.PostgresJsonStatement.jsonColumnNames;
import static com.akiban.sql.pg.PostgresJsonStatement.jsonColumnTypes;
import static com.akiban.sql.pg.PostgresServerSession.OutputFormat;

import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerRoutineInvocation;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.io.IOException;

public abstract class PostgresJavaRoutine extends PostgresDMLStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresJavaRoutine: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresJavaRoutine: acquire shared lock");

    protected ServerRoutineInvocation invocation;

    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerRoutineInvocation invocation,
                                              List<ParameterNode> params, 
                                              int[] paramTypes) {
        Routine routine = invocation.getRoutine();
        List<String> columnNames;
        List<PostgresType> columnTypes;
        switch (server.getOutputFormat()) {
        case JSON:
        case JSON_WITH_META_DATA:
            columnNames = jsonColumnNames();
            columnTypes = jsonColumnTypes();
            break;
        default:
            columnTypes = columnTypes(routine);
            if (columnTypes.isEmpty()) {
                columnTypes = null;
                columnNames = null;
            }
            else {
                columnNames = columnNames(routine);
            }
            break;
        }
        PostgresType[] parameterTypes;
        if ((params == null) || params.isEmpty())
            parameterTypes = null;
        else
            parameterTypes = parameterTypes(invocation, params.size(), paramTypes);
        boolean usesPValues = server.getBooleanProperty("newtypes", Types3Switch.ON);
        switch (routine.getCallingConvention()) {
        case JAVA:
            return PostgresJavaMethod.statement(server, invocation, 
                                                columnNames, columnTypes,
                                                parameterTypes, usesPValues);
        case SCRIPT_FUNCTION_JAVA:
            return PostgresScriptFunctionJavaRoutine.statement(server, invocation, 
                                                               columnNames, columnTypes,
                                                               parameterTypes, usesPValues);
        case SCRIPT_BINDINGS:
            return PostgresScriptBindingsRoutine.statement(server, invocation, 
                                                           columnNames, columnTypes,
                                                           parameterTypes, usesPValues);
        default:
            return null;
        }
    }

    protected PostgresJavaRoutine(ServerRoutineInvocation invocation,
                                  List<String> columnNames, 
                                  List<PostgresType> columnTypes,
                                  PostgresType[] parameterTypes,
                                  boolean usesPValues) {
        super(null, columnNames, columnTypes, parameterTypes, usesPValues);
        this.invocation = invocation;
    }

    protected abstract ServerJavaRoutine javaRoutine(PostgresQueryContext context);

    public ServerRoutineInvocation getInvocation() {
        return invocation;
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    protected InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
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
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        Queue<ResultSet> dynamicResultSets = null;
        ServerJavaRoutine call = javaRoutine(context);
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
                outputter.output(call, usesPValues());
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
                        outputter.setMetaData(rs.getMetaData());
                        outputter.sendDescription();
                        while (rs.next()) {
                            outputter.output(rs, usesPValues());
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
            if (dynamicResultSets != null) {
                while (!dynamicResultSets.isEmpty()) {
                    try {
                        dynamicResultSets.remove().close();
                    }
                    catch (SQLException ex) {
                    }
                }
            }
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
        List<String> result = new ArrayList<String>();
        for (Parameter param : routine.getParameters()) {
            if (param.getDirection() == Parameter.Direction.IN) continue;
            String name = param.getName();
            if (name == null)
                name = String.format("col%d", result.size() + 1);
            result.add(name);
        }
        return result;
    }

    public static List<PostgresType> columnTypes(Routine routine) {
        List<PostgresType> result = new ArrayList<PostgresType>();
        for (Parameter param : routine.getParameters()) {
            if (param.getDirection() == Parameter.Direction.IN) continue;
            result.add(PostgresType.fromAIS(param));
        }
        return result;
    }

    public static PostgresType[] parameterTypes(ServerRoutineInvocation invocation,
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
                        pgType = new PostgresType(oid, (short)-1, -1, null, null);
                    else
                        pgType = new PostgresType(oid,  (short)-1, -1, 
                                                  pgType.getAkType(),
                                                  pgType.getInstance());
                }
            }
            result[i] = pgType;
        }
        return result;
    }

}
