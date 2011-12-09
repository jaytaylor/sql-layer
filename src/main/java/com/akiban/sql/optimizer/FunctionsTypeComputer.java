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
        case NodeTypes.LIKE_OPERATOR_NODE:
        case NodeTypes.LOCATE_FUNCTION_NODE:
        case NodeTypes.SUBSTRING_OPERATOR_NODE:
        case NodeTypes.TRIM_OPERATOR_NODE:
        case NodeTypes.TIMESTAMP_ADD_FN_NODE:
        case NodeTypes.TIMESTAMP_DIFF_FN_NODE:
            return ternaryOperatorFunction((TernaryOperatorNode)node);
        default:
            return super.computeType(node);
        }
    }

    // Access to typed function arguments.
    interface ArgumentsAccess {
        public int nargs();
        public ExpressionType argType(int index) throws StandardException;
        public ExpressionType addCast(int index, 
                                      ExpressionType argType, AkType requiredType)
                throws StandardException;
    }

    // Compute type from function's composer with arguments' types.
    protected DataTypeDescriptor expressionComposer(String functionName,
                                                    ArgumentsAccess args)
            throws StandardException {
        ExpressionComposer composer;
        try {
            composer = functionsRegistry.composer(functionName);
        }
        catch (NoSuchFunctionException ex) {
            return null;
        }
        int nargs = args.nargs();
        List<ExpressionType> argTypes = new ArrayList<ExpressionType>(nargs);
        List<AkType> origTypes = new ArrayList<AkType>(nargs);
        for (int i = 0; i < nargs; i++) {
            ExpressionType argType = args.argType(i);
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
            argTypes.set(i, args.addCast(i, argTypes.get(i), requiredTypes.get(i)));
        }
        ExpressionType resultType = composer.composeType(argTypes);
        if (resultType == null)
            return null;
        return fromExpressionType(resultType);
    }

    protected DataTypeDescriptor noArgFunction(String functionName) 
            throws StandardException {
        DataTypeDescriptor result = 
            expressionComposer(functionName,
                               new ArgumentsAccess() {
                                   @Override
                                   public int nargs() {
                                       return 0;
                                   }

                                   @Override
                                   public ExpressionType argType(int index) {
                                       assert false;
                                       return null;
                                   }

                                   @Override
                                   public ExpressionType addCast(int index, 
                                                                 ExpressionType argType, 
                                                                 AkType requiredType) {
                                       assert false;
                                       return null;
                                   }
                               });
        if (result == null) {
            // Not a regular function, maybe an environment access function.
            EnvironmentExpressionFactory environment;
            try {
               environment = functionsRegistry.environment(functionName);
            }
            catch (NoSuchFunctionException ex) {
                environment = null;
            }
            if (environment != null)
                result = fromExpressionType(environment.getType());
        }
        return result;
    }

    protected DataTypeDescriptor unaryOperatorFunction(UnaryOperatorNode node) 
            throws StandardException {
        return expressionComposer(node.getMethodName(), new UnaryValuesAccess(node));
    }

    protected DataTypeDescriptor binaryOperatorFunction(BinaryOperatorNode node) 
            throws StandardException {
        return expressionComposer(node.getMethodName(), new BinaryValuesAccess(node));
    }

    protected DataTypeDescriptor ternaryOperatorFunction(TernaryOperatorNode node) 
            throws StandardException {
        return expressionComposer(node.getMethodName(), new TernaryValuesAccess(node));
    }

    // Normal AST nodes for arguments.
    abstract class ValueNodesAccess implements ArgumentsAccess {
        public abstract ValueNode argNode(int index);
        public abstract void setArgNode(int index, ValueNode value);

        @Override
        public ExpressionType argType(int index) {
            return valueExpressionType(argNode(index));
        }

        @Override
        public ExpressionType addCast(int index, 
                                      ExpressionType argType, AkType requiredType) 
                throws StandardException {
            ValueNode value = argNode(index);
            ExpressionType castType = castType(argType, requiredType, value.getType());
            DataTypeDescriptor sqlType = fromExpressionType(castType);
            if (value instanceof ParameterNode) {
                value.setType(sqlType);
            }
            else {
                value = (ValueNode)value.getNodeFactory()
                    .getNode(NodeTypes.CAST_NODE, 
                             value, sqlType, value.getParserContext());
                setArgNode(index, value);
            }
            return castType;
        }
    }

    final class UnaryValuesAccess extends ValueNodesAccess {
        private final UnaryOperatorNode node;

        public UnaryValuesAccess(UnaryOperatorNode node) {
            this.node = node;
        }

        @Override
        public int nargs() {
            return 1;
        }

        @Override
        public ValueNode argNode(int index) {
            assert (index == 0);
            return node.getOperand();
        }

        @Override
        public void setArgNode(int index, ValueNode value) {
            assert (index == 0);
            node.setOperand(value);
        }
    }

    final class BinaryValuesAccess extends ValueNodesAccess {
        private final BinaryOperatorNode node;

        public BinaryValuesAccess(BinaryOperatorNode node) {
            this.node = node;
        }

        @Override
        public int nargs() {
            return 2;
        }

        @Override
        public ValueNode argNode(int index) {
            switch (index) {
            case 0:
                return node.getLeftOperand();
            case 1:
                return node.getRightOperand();
            default:
                assert false;
                return null;
            }
        }

        @Override
        public void setArgNode(int index, ValueNode value) {
            switch (index) {
            case 0:
                node.setLeftOperand(value);
                break;
            case 1: 
                node.setRightOperand(value); 
                break;
           default:
                assert false;
            }
        }
    }

    final class TernaryValuesAccess extends ValueNodesAccess {
        private final TernaryOperatorNode node;

        public TernaryValuesAccess(TernaryOperatorNode node) {
            this.node = node;
        }

        @Override
        public int nargs() {
            if (node.getRightOperand() != null)
                return 3;
            else
                return 2;
        }

        @Override
        public ValueNode argNode(int index) {
            switch (index) {
            case 0:
                return node.getReceiver();
            case 1:
                return node.getLeftOperand();
            case 2:
                return node.getRightOperand();
            default:
                assert false;
                return null;
            }
        }

        @Override
        public void setArgNode(int index, ValueNode value) {
            switch (index) {
            case 0:
                node.setReceiver(value);
                break;
            case 1:
                node.setLeftOperand(value);
                break;
            case 2:
                node.setRightOperand(value);
                break;
            default:
                assert false;
            }
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
            return expressionComposer(methodCall.getMethodName(),
                                      new JavaValuesAccess(methodCall.getMethodParameters()));
        }
    }

    final class JavaValuesAccess implements ArgumentsAccess {
        private final JavaValueNode[] args;

        public JavaValuesAccess(JavaValueNode[] args) {
            this.args = args;
        }

        @Override
        public int nargs() {
            if (args == null)
                return 0;
            else
                return args.length;
        }

        @Override
        public ExpressionType argType(int index) throws StandardException {
            JavaValueNode arg = args[index];
            if (arg instanceof SQLToJavaValueNode)
                return valueExpressionType(((SQLToJavaValueNode)arg).getSQLValueNode());
            else
                return toExpressionType(arg.getType());
        }

        @Override
        public ExpressionType addCast(int index,
                                      ExpressionType argType, AkType requiredType) 
                throws StandardException {
            JavaValueNode arg = args[index];
            if (arg instanceof SQLToJavaValueNode) {
                SQLToJavaValueNode jarg = (SQLToJavaValueNode)arg;
                ValueNode sqlArg = jarg.getSQLValueNode();
                ExpressionType castType = castType(argType, requiredType, 
                                                   sqlArg.getType());
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
                return castType;
            }
            else
                return argType;
        }
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
            if (sqlType != null)
                return ExpressionTypes.varchar(sqlType.getMaximumWidth());
            else
                return ExpressionTypes.varchar(TypeId.VARCHAR_ID.getMaximumMaximumWidth());
        case VARBINARY:
            if (sqlType != null)
                return ExpressionTypes.varbinary(sqlType.getMaximumWidth());
            else
                return ExpressionTypes.varbinary(TypeId.VARBIT_ID.getMaximumMaximumWidth());
        case DECIMAL:
            if (sqlType != null) {
                TypeId typeId = sqlType.getTypeId();
                if (typeId.isNumericTypeId())
                    return ExpressionTypes.decimal(sqlType.getPrecision(),
                                                   sqlType.getScale());
                else
                    return ExpressionTypes.decimal(typeId.getMaximumPrecision(),
                                                   typeId.getMaximumScale());
            }
            else
                return ExpressionTypes.decimal(TypeId.DECIMAL_ID.getMaximumPrecision(),
                                               TypeId.DECIMAL_ID.getMaximumScale());
        default:
            return ExpressionTypes.newType(toType, 0, 0);
        }
    }

}
