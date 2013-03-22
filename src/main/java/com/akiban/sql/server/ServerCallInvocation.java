/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.server;

import com.akiban.sql.parser.ConstantNode;
import com.akiban.sql.parser.JavaValueNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.SQLToJavaValueNode;
import com.akiban.sql.parser.StaticMethodCallNode;
import com.akiban.sql.parser.ValueNode;

import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

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

    public void copyParameters(ServerQueryContext source, ServerQueryContext target,
                               boolean usePVals) {
        if (usePVals) {
            for (int i = 0; i < parameterArgs.length; i++) {
                if (parameterArgs[i] < 0) {
                    AkType nullType = null;
                    if (constantArgs[i] == null)
                        nullType = getAkType(i);
                    target.setPValue(i, PValueSources.fromObject(constantArgs[i], nullType).value());
                }
                else {
                    target.setPValue(i, source.getPValue(parameterArgs[i]));
                }
            }
        }
        else {
            FromObjectValueSource value = new FromObjectValueSource();
            for (int i = 0; i < parameterArgs.length; i++) {
                if (parameterArgs[i] < 0) {
                    value.setReflectively(constantArgs[i]);
                    target.setValue(i, value);
                }
                else {
                    target.setValue(i, source.getValue(parameterArgs[i]));
                }
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
    public ServerJavaValues asValues(ServerQueryContext parameters) {
        return new Values(parameters);
    }

    protected class Values extends ServerJavaValues {
        private ServerQueryContext parameters;

        protected Values(ServerQueryContext parameters) {
            this.parameters = parameters;
        }

        @Override
        protected int size() {
            return getRoutine().getParameters().size();
        }

        @Override
        protected ServerQueryContext getContext() {
            return parameters;
        }

        @Override
        protected ValueSource getValue(int index) {
            if (parameterArgs[index] < 0) {
                return new FromObjectValueSource().setReflectively(constantArgs[index]);
            }
            else {
                return parameters.getValue(parameterArgs[index]);
            }
        }

        @Override
        protected PValueSource getPValue(int index) {
            TInstance tinstance = getTInstance(index);
            TClass tclass = tinstance.typeClass();
            PValueSource source;
            if (parameterArgs[index] < 0) {
                TPreptimeValue value = PValueSources.fromObject(constantArgs[index], null);
                source = value.value();
                if (value.instance().typeClass().equals(tclass))
                    return source; // Literal value matches.
            }
            else {
                source = parameters.getPValue(parameterArgs[index]);
            }
            // Constants passed or parameters bound may not be of the
            // type specified in the signature.
            PValue pvalue = new PValue(tinstance);
            TExecutionContext executionContext = 
                new TExecutionContext(null, null, tinstance,
                                      parameters, null, null, null);
            tclass.fromObject(executionContext, source, pvalue);
            return pvalue;
        }

        @Override
        protected AkType getAkType(int index) {
            return ServerCallInvocation.this.getAkType(index);
        }

        @Override
        protected TInstance getTInstance(int index) {
            return ServerCallInvocation.this.getTInstance(index);
        }

        @Override
        protected void setValue(int index, ValueSource source, AkType akType) {
            if (parameterArgs[index] < 0) {
                // An INOUT passed as a constant; do not overwrite it.
            }
            else {
                parameters.setValue(parameterArgs[index], source, akType);
            }
        }

        @Override
        protected void setPValue(int index, PValueSource source) {
            if (parameterArgs[index] < 0) {
                // An INOUT passed as a constant; do not overwrite it.
            }
            else {
                parameters.setPValue(parameterArgs[index], source);
            }
        }

        @Override
        protected java.sql.ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

}
