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

package com.foundationdb.sql.server;

import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.JavaValueNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.SQLToJavaValueNode;
import com.foundationdb.sql.parser.StaticMethodCallNode;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.pvalue.PValue;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueSources;

import java.util.Arrays;

public class ServerCallInvocation extends ServerRoutineInvocation
{
    private final Object[] constantArgs;
    private final int[] parameterArgs;

    protected ServerCallInvocation(Routine routine,
                                   Object[] constantArgs,
                                   int[] parameterArgs) {
        super(routine);
        this.constantArgs = constantArgs;
        this.parameterArgs = parameterArgs;
    }

    public static ServerCallInvocation of(ServerSession server,
                                          StaticMethodCallNode methodCall) {
        String schemaName, routineName;
        if (methodCall.getProcedureName() == null) {
            schemaName = null;
            routineName = methodCall.getMethodName();
        }
        else {
            schemaName = methodCall.getProcedureName().getSchemaName();
            routineName = methodCall.getProcedureName().getTableName();
        }
        if (schemaName == null) {
            schemaName = server.getDefaultSchemaName();
        }
        Routine routine = server.getAIS().getRoutine(schemaName, routineName);
        if (routine == null) 
            throw new NoSuchRoutineException(schemaName, routineName);
        Object[] constantArgs = null;
        int[] parameterArgs = null;
        JavaValueNode[] margs = methodCall.getMethodParameters();
        if (margs != null) {
            constantArgs = new Object[margs.length];
            parameterArgs = new int[margs.length];
            Arrays.fill(parameterArgs, -1);
            for (int i = 0; i < margs.length; i++) {
                JavaValueNode marg = margs[i];
                if (marg instanceof SQLToJavaValueNode) {
                    ValueNode sqlArg = ((SQLToJavaValueNode)marg).getSQLValueNode();
                    if (sqlArg instanceof ConstantNode) {
                        constantArgs[i] = ((ConstantNode)sqlArg).getValue();
                        continue;
                    }
                    else if (sqlArg instanceof ParameterNode) {
                        parameterArgs[i] = ((ParameterNode)sqlArg).getParameterNumber();
                        continue;
                    }
                }
                throw new UnsupportedSQLException("CALL parameter must be constant",
                                                  marg);
            }
        }
        return new ServerCallInvocation(routine, constantArgs, parameterArgs);
    }

    public static ServerCallInvocation of(ServerSession server, TableName routineName) {
        Routine routine = server.getAIS().getRoutine(routineName);
        if (routine == null) 
            throw new NoSuchRoutineException(routineName);
        int nparams = routine.getParameters().size();
        Object[] constantArgs = new Object[nparams];
        int[] parameterArgs = new int[nparams];
        for (int i = 0; i < nparams; i++) {
            parameterArgs[i] = i;
        }
        return new ServerCallInvocation(routine, constantArgs, parameterArgs);
    }

    public int parameterUsage(int param) {
        if (parameterArgs != null) {
            for (int i = 0; i < parameterArgs.length; i++) {
                if (parameterArgs[i] == param) {
                    return i;
                }
            }
        }
        return -1;
    }

    public boolean hasParameters() {
        for (int index : parameterArgs) {
            if (index != -1) {
                return true;
            }
        }
        return false;
    }

    public boolean parametersInOrder() {
        for (int i = 0; i < parameterArgs.length; i++) {
            if (i != parameterArgs[i]) {
                return false;
            }
        }
        return true;
    }

    public void copyParameters(QueryBindings source, QueryBindings target) {
        for (int i = 0; i < parameterArgs.length; i++) {
            if (parameterArgs[i] < 0) {
                TInstance tInstance = null;
                if (constantArgs[i] == null)
                    tInstance = this.getTInstance(i);
                target.setPValue(i, PValueSources.pValuefromObject(constantArgs[i], tInstance));
            }
            else {
                target.setPValue(i, source.getPValue(parameterArgs[i]));
            }
        }
    }

    @Override
    public int size() {
        if (constantArgs == null)
            return 0;
        else
            return constantArgs.length;
    }

    public int getParameterNumber(int i) {
        return parameterArgs[i];
    }

    public Object getConstantValue(int i) {
        return constantArgs[i];
    }

    @Override
    public ServerJavaValues asValues(ServerQueryContext context, QueryBindings bindings) {
        return new Values(context, bindings);
    }

    protected class Values extends ServerJavaValues {
        private ServerQueryContext context;
        private QueryBindings bindings;

        protected Values(ServerQueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        protected int size() {
            return getRoutine().getParameters().size();
        }

        @Override
        protected ServerQueryContext getContext() {
            return context;
        }

        @Override
        protected PValueSource getPValue(int index) {
            TInstance tinstance = getTInstance(index);
            TClass tclass = tinstance.typeClass();
            PValueSource source;
            if (parameterArgs[index] < 0) {
                source = PValueSources.pValuefromObject(constantArgs[index], tinstance);
                if (source.tInstance().typeClass().equals(tclass))
                    return source; // Literal value matches.
            }
            else {
                source = bindings.getPValue(parameterArgs[index]);
            }
            // Constants passed or parameters bound may not be of the
            // type specified in the signature.
            PValue pvalue = new PValue(tinstance);
            TExecutionContext executionContext = 
                new TExecutionContext(null, null, tinstance,
                                      context, null, null, null);
            tclass.fromObject(executionContext, source, pvalue);
            return pvalue;
        }

        @Override
        protected TInstance getTInstance(int index) {
            return ServerCallInvocation.this.getTInstance(index);
        }

        @Override
        protected void setPValue(int index, PValueSource source) {
            if (parameterArgs[index] < 0) {
                // An INOUT passed as a constant; do not overwrite it.
            }
            else {
                bindings.setPValue(parameterArgs[index], source);
            }
        }

        @Override
        protected java.sql.ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

}
