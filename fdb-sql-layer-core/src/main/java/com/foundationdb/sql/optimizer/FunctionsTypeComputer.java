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

package com.foundationdb.sql.optimizer;

import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.ais.model.Routine;

import com.foundationdb.sql.optimizer.plan.AggregateFunctionExpression;

/** Marks aggregate functions as such. */
public class FunctionsTypeComputer extends AISTypeComputer
{
    private TypesRegistryService functionsRegistry;
    private boolean useComposers;

    public FunctionsTypeComputer(TypesRegistryService functionsRegistry) {
        this.functionsRegistry = functionsRegistry;
        useComposers = false;
    }
    
    public boolean isUseComposers() {
        return useComposers;
    }

    public void setUseComposers(boolean useComposers) {
        this.useComposers = useComposers;
    }

    @Override
    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.JAVA_TO_SQL_VALUE_NODE:
            return javaValueNode(((JavaToSQLValueNode)node).getJavaValueNode());
        default:
            return super.computeType(node);
        }
    }

    protected DataTypeDescriptor javaValueNode(JavaValueNode javaValue)
            throws StandardException {
        if (javaValue instanceof MethodCallNode) {
            return methodCallNode((MethodCallNode)javaValue);
        }
        else {
            return null;
        }
    }

    protected DataTypeDescriptor methodCallNode(MethodCallNode methodCall)
            throws StandardException {
        if (methodCall.getUserData() != null) {
            Routine routine = (Routine)methodCall.getUserData();
            return routine.getReturnValue().getType().dataTypeDescriptor();
        }
        if (methodCall.getMethodParameters().length == 1) {
            return oneArgMethodCall(methodCall);
        }
        else {
            return null;
        }
    }

    protected DataTypeDescriptor oneArgMethodCall(MethodCallNode methodCall)
            throws StandardException {
        TypesRegistryService.FunctionKind functionKind =
            functionsRegistry.getFunctionKind(methodCall.getMethodName());
        if (functionKind == TypesRegistryService.FunctionKind.AGGREGATE) {
            // Mark the method call as really an aggregate function.
            // Could do the substitution now, but that would require throwing
            // a subclass of StandardException up to visit() or something other
            // complicated control flow.
            methodCall.setJavaClassName(AggregateFunctionExpression.class.getName());
            JavaValueNode arg = methodCall.getMethodParameters()[0];
            if (arg instanceof SQLToJavaValueNode) {
                SQLToJavaValueNode jarg = (SQLToJavaValueNode)arg;
                ValueNode sqlArg = jarg.getSQLValueNode();
                return sqlArg.getType();
            }
        }
        return null;
    }

    /** Return the name of a built-in special function. */
    public static String specialFunctionName(SpecialFunctionNode node) {
        switch (node.getNodeType()) {
        case NodeTypes.USER_NODE:
        case NodeTypes.CURRENT_USER_NODE:
            return "current_user";
        case NodeTypes.SESSION_USER_NODE:
            return "session_user";
        case NodeTypes.SYSTEM_USER_NODE:
            return "system_user";
        case NodeTypes.CURRENT_SCHEMA_NODE:
            return "current_schema";
        case NodeTypes.CURRENT_ISOLATION_NODE:
        case NodeTypes.IDENTITY_VAL_NODE:
        case NodeTypes.CURRENT_ROLE_NODE:
        default:
            return null;
        }
    }

    /** Return the name of a built-in special function. */
    public static String currentDatetimeFunctionName(CurrentDatetimeOperatorNode node) {
        switch (node.getField()) {
        case DATE:
            return "current_date";
        case TIME:
            return "current_time";
        case TIMESTAMP:
            return "current_timestamp";
        default:
            return null;
        }
    }

}
