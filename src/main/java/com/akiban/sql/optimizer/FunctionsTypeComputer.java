/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.server.expression.EnvironmentExpressionFactory;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.service.functions.FunctionsRegistry;

import com.akiban.server.types.AkType;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchFunctionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Calculate types from expression composers. */
public class FunctionsTypeComputer extends AISTypeComputer
{
    private FunctionsRegistry functionsRegistry;

    public FunctionsTypeComputer(FunctionsRegistry functionsRegistry) {
        this.functionsRegistry = functionsRegistry;
    }
    
    @Override
    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.JAVA_TO_SQL_VALUE_NODE:
            return javaValueNode(((JavaToSQLValueNode)node).getJavaValueNode());
        case NodeTypes.USER_NODE:
        case NodeTypes.CURRENT_USER_NODE:
        case NodeTypes.SESSION_USER_NODE:
        case NodeTypes.SYSTEM_USER_NODE:
        case NodeTypes.CURRENT_ISOLATION_NODE:
        case NodeTypes.IDENTITY_VAL_NODE:
        case NodeTypes.CURRENT_SCHEMA_NODE:
        case NodeTypes.CURRENT_ROLE_NODE:
            return specialFunctionNode((SpecialFunctionNode)node);
        case NodeTypes.CURRENT_DATETIME_OPERATOR_NODE:
            return currentDatetimeOperatorNode((CurrentDatetimeOperatorNode)node);
        case NodeTypes.DB2_LENGTH_OPERATOR_NODE:
        case NodeTypes.EXTRACT_OPERATOR_NODE:
        case NodeTypes.CHAR_LENGTH_OPERATOR_NODE:
        case NodeTypes.SIMPLE_STRING_OPERATOR_NODE:
        case NodeTypes.UNARY_DATE_TIMESTAMP_OPERATOR_NODE:
            return unaryOperatorFunction((UnaryOperatorNode)node);
        default:
            return super.computeType(node);
        }
    }

    protected DataTypeDescriptor javaValueNode(JavaValueNode javaValue)
            throws StandardException {
        if (javaValue instanceof MethodCallNode) {
            return methodCallNode((MethodCallNode)javaValue);
        }
        else if (javaValue instanceof SQLToJavaValueNode) {
            return computeType(((SQLToJavaValueNode)javaValue).getSQLValueNode());
        }
        else {
            return null;
        }
    }

    protected DataTypeDescriptor methodCallNode(MethodCallNode methodCall)
            throws StandardException {
        if ((methodCall.getMethodParameters() == null) ||
            (methodCall.getMethodParameters().length == 0)) {
            return noArgFunction(methodCall.getMethodName());
        }
        else {
            ExpressionComposer composer;
            try {
                composer = functionsRegistry.composer(methodCall.getMethodName());
            }
            catch (NoSuchFunctionException ex) {
                composer = null;
            }
            if (composer != null)
                return expressionComposer(composer, methodCall.getMethodParameters());
        }
        return null;
    }

    protected DataTypeDescriptor noArgFunction(String functionName) 
            throws StandardException {
        {
            ExpressionComposer composer;
            try {
                composer = functionsRegistry.composer(functionName);
            }
            catch (NoSuchFunctionException ex) {
                composer = null;
            }
            if (composer != null)
                return expressionComposer(composer, null);
        }
        {
            EnvironmentExpressionFactory environment;
            try {
               environment = functionsRegistry.environment(functionName);
            }
            catch (NoSuchFunctionException ex) {
                environment = null;
            }
            if (environment != null)
                return fromExpressionType(environment.getType());
        }
        return null;
    }

    protected DataTypeDescriptor expressionComposer(ExpressionComposer composer,
                                                    JavaValueNode[] args)
            throws StandardException {
        int nargs = 0;
        if (args != null)
            nargs = args.length;
        List<ExpressionType> argTypes = new ArrayList<ExpressionType>(nargs);
        List<AkType> origTypes = new ArrayList<AkType>(nargs);
        for (int i = 0; i < nargs; i++) {
            JavaValueNode arg = args[i];
            ExpressionType argType;
            if (arg instanceof SQLToJavaValueNode)
                argType = valueExpressionType(((SQLToJavaValueNode)arg).getSQLValueNode());
            else
                argType = toExpressionType(arg.getType());
            if (argType == null)
                return null;
            argTypes.add(argType);
            origTypes.add(argType.getType());
        }
        List<AkType> requiredTypes = new ArrayList<AkType>(origTypes);
        composer.argumentTypes(requiredTypes);
        for (int i = 0; i < nargs; i++) {
            if ((origTypes.get(i) == requiredTypes.get(i)) ||
                (origTypes.get(i) == AkType.NULL))
                continue;
            // Need a different type: add a CAST.
            JavaValueNode arg = args[i];
            ExpressionType castType = castType(argTypes.get(i),
                                               requiredTypes.get(i), 
                                               arg.getType());
            if (arg instanceof SQLToJavaValueNode) {
                SQLToJavaValueNode jarg = (SQLToJavaValueNode)arg;
                ValueNode sqlArg = jarg.getSQLValueNode();
                DataTypeDescriptor sqlType = fromExpressionType(castType);
                if (sqlArg instanceof ParameterNode) {
                    sqlArg.setType(sqlType);
                }
                else {
                    ValueNode cast = (ValueNode)sqlArg.getNodeFactory()
                        .getNode(NodeTypes.CAST_NODE, 
                                 sqlArg, sqlType, sqlArg.getParserContext());
                    jarg.setSQLValueNode(cast);
                }
            }
            argTypes.set(i, castType);
        }
        ExpressionType resultType = composer.composeType(argTypes);
        if (resultType == null)
            return null;
        return fromExpressionType(resultType);
    }

    protected DataTypeDescriptor unaryOperatorFunction(UnaryOperatorNode node) 
            throws StandardException {
        ExpressionComposer composer;
        try {
            composer = functionsRegistry.composer(node.getMethodName());
        }
        catch (NoSuchFunctionException ex) {
            return null;
        }
        ExpressionType argType = valueExpressionType(node.getOperand());
        if (argType == null)
            return null;
        List<ExpressionType> argTypes = Collections.singletonList(argType);
        ExpressionType resultType = composer.composeType(argTypes);
        if (resultType == null)
            return null;
        return fromExpressionType(resultType);
    }

    protected DataTypeDescriptor specialFunctionNode(SpecialFunctionNode node)
            throws StandardException {
        return noArgFunction(specialFunctionName(node));
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
        case NodeTypes.CURRENT_ISOLATION_NODE:
        case NodeTypes.IDENTITY_VAL_NODE:
        case NodeTypes.CURRENT_SCHEMA_NODE:
        case NodeTypes.CURRENT_ROLE_NODE:
        default:
            return null;
        }
    }

    protected DataTypeDescriptor currentDatetimeOperatorNode(CurrentDatetimeOperatorNode node)
            throws StandardException {
        return noArgFunction(currentDatetimeFunctionName(node));
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

    protected ExpressionType valueExpressionType(ValueNode value) {
        DataTypeDescriptor type = value.getType();
        if (type == null) {
            if (value instanceof UntypedNullConstantNode) {
                // Give composer a change to establish type of null.
                return ExpressionTypes.NULL;
            }
            if (value instanceof ParameterNode) {
                // Likewise parameters.
                return ExpressionTypes.UNSUPPORTED;
            }
        }
        return toExpressionType(type);
    }

    /* Yet another translator between type regimes. */

    protected ExpressionType toExpressionType(DataTypeDescriptor sqlType) {
        if (sqlType == null)
            return null;
        TypeId typeId = sqlType.getTypeId();
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            return ExpressionTypes.BOOL;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            return ExpressionTypes.varchar(sqlType.getMaximumWidth());
        case TypeId.FormatIds.DATE_TYPE_ID:
            return ExpressionTypes.DATE;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            return ExpressionTypes.decimal(sqlType.getPrecision(),
                                           sqlType.getScale());
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            return ExpressionTypes.DOUBLE;
        case TypeId.FormatIds.INT_TYPE_ID:
            return ExpressionTypes.INT;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            return ExpressionTypes.LONG;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
        case TypeId.FormatIds.BLOB_TYPE_ID:
        case TypeId.FormatIds.CLOB_TYPE_ID:
        case TypeId.FormatIds.XML_TYPE_ID:
            return ExpressionTypes.TEXT;
        case TypeId.FormatIds.REAL_TYPE_ID:
            return ExpressionTypes.FLOAT;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            return ExpressionTypes.INT;
        case TypeId.FormatIds.TIME_TYPE_ID:
            return ExpressionTypes.TIME;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            return ExpressionTypes.TIMESTAMP;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            return ExpressionTypes.varbinary(sqlType.getMaximumWidth());
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            return ExpressionTypes.varchar(sqlType.getMaximumWidth());
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            return ExpressionTypes.newType(AkType.valueOf(sqlType.getFullSQLTypeName().toUpperCase()), 
                                           sqlType.getPrecision(), sqlType.getScale());
        default:
            return null;
        }
    }

    protected DataTypeDescriptor fromExpressionType(ExpressionType resultType) {
        switch (resultType.getType()) {
        case BOOL:
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
        case INT:
            return new DataTypeDescriptor(TypeId.INTEGER_ID, true);
        case LONG:
            return new DataTypeDescriptor(TypeId.BIGINT_ID, true);
        case DOUBLE:
            return new DataTypeDescriptor(TypeId.DOUBLE_ID, true);
        case FLOAT:
            return new DataTypeDescriptor(TypeId.REAL_ID, true);
        case U_INT:
            return new DataTypeDescriptor(TypeId.INTEGER_UNSIGNED_ID, true);
        case U_BIGINT:
            return new DataTypeDescriptor(TypeId.BIGINT_UNSIGNED_ID, true);
        case U_FLOAT:
            return new DataTypeDescriptor(TypeId.REAL_UNSIGNED_ID, true);
        case U_DOUBLE:
            return new DataTypeDescriptor(TypeId.DOUBLE_UNSIGNED_ID, true);
        case DATE:
            return new DataTypeDescriptor(TypeId.DATE_ID, true);
        case TIME:
            return new DataTypeDescriptor(TypeId.TIME_ID, true);
        case TIMESTAMP:
            return new DataTypeDescriptor(TypeId.TIMESTAMP_ID, true);
        case VARCHAR:
            return new DataTypeDescriptor(TypeId.VARCHAR_ID, true, 
                                          resultType.getPrecision());
        case DECIMAL:
            {
                int precision = resultType.getPrecision();
                int scale = resultType.getScale();
                return new DataTypeDescriptor(TypeId.DECIMAL_ID, precision, scale, true,
                                              DataTypeDescriptor.computeMaxWidth(precision, scale));
            }
        case TEXT:
            return new DataTypeDescriptor(TypeId.LONGVARCHAR_ID, true);
        case VARBINARY:
            return new DataTypeDescriptor(TypeId.LONGVARBIT_ID, true);
        case NULL:
            return null;
        case DATETIME:
        case YEAR:
        default:
            try {
                return new DataTypeDescriptor(TypeId.getUserDefinedTypeId(null,
                                                                          resultType.getType().name(),
                                                                          null),
                                              true);
            }
            catch (StandardException ex) {
                throw new AkibanInternalException("Cannot make type for " + resultType,
                                                  ex);
            }
        }
    }

    protected ExpressionType castType(ExpressionType fromType, AkType toType,
                                      DataTypeDescriptor sqlType) {
        switch (toType) {
        case BOOL:
            return ExpressionTypes.BOOL;
        case INT:
            return ExpressionTypes.INT;
        case YEAR:
            return ExpressionTypes.YEAR;
        case LONG:
            return ExpressionTypes.LONG;
        case DOUBLE:
            return ExpressionTypes.DOUBLE;
        case FLOAT:
            return ExpressionTypes.FLOAT;
        case U_INT:
            return ExpressionTypes.U_INT;
        case U_BIGINT:
            return ExpressionTypes.U_BIGINT;
        case U_FLOAT:
            return ExpressionTypes.U_FLOAT;
        case U_DOUBLE:
            return ExpressionTypes.U_DOUBLE;
        case DATE:
            return ExpressionTypes.DATE;
        case TIME:
            return ExpressionTypes.TIME;
        case DATETIME:
            return ExpressionTypes.DATETIME;
        case TIMESTAMP:
            return ExpressionTypes.TIMESTAMP;
        case TEXT:
            return ExpressionTypes.TEXT;
        case VARCHAR:
            return ExpressionTypes.varchar(sqlType.getMaximumWidth());
        case VARBINARY:
            return ExpressionTypes.varbinary(sqlType.getMaximumWidth());
        case DECIMAL:
            {
                TypeId typeId = sqlType.getTypeId();
                if (typeId.isNumericTypeId())
                    return ExpressionTypes.decimal(sqlType.getPrecision(),
                                                   sqlType.getScale());
                else
                    return ExpressionTypes.decimal(typeId.getMaximumPrecision(),
                                                   typeId.getMaximumScale());
            }
        default:
            return ExpressionTypes.newType(toType, 0, 0);
        }
    }

}
