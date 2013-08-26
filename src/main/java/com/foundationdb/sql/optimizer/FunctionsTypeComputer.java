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

import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.expression.std.ExpressionTypes;
import com.foundationdb.server.service.functions.FunctionsRegistry;
import com.foundationdb.server.t3expressions.T3RegistryService;
import com.foundationdb.server.types.AkType;

import com.foundationdb.server.error.NoSuchFunctionException;

import com.foundationdb.sql.optimizer.plan.AggregateFunctionExpression;

import static com.foundationdb.sql.optimizer.TypesTranslation.*;

/** Calculate types from expression composers. */
public class FunctionsTypeComputer extends AISTypeComputer
{
    //private FunctionsRegistry functionsRegistry;
    private T3RegistryService functionsRegistry;
    private boolean useComposers;

    public FunctionsTypeComputer(T3RegistryService functionsRegistry) {
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
        case NodeTypes.OCTET_LENGTH_OPERATOR_NODE:
        case NodeTypes.EXTRACT_OPERATOR_NODE:
        case NodeTypes.CHAR_LENGTH_OPERATOR_NODE:
        case NodeTypes.SIMPLE_STRING_OPERATOR_NODE:
        case NodeTypes.UNARY_DATE_TIMESTAMP_OPERATOR_NODE:
        case NodeTypes.ABSOLUTE_OPERATOR_NODE:
        case NodeTypes.SQRT_OPERATOR_NODE:
        case NodeTypes.UNARY_PLUS_OPERATOR_NODE:
        case NodeTypes.UNARY_MINUS_OPERATOR_NODE:
        case NodeTypes.UNARY_BITNOT_OPERATOR_NODE:
            return unaryOperatorFunction((UnaryOperatorNode)node);
        case NodeTypes.LIKE_OPERATOR_NODE:
        case NodeTypes.LOCATE_FUNCTION_NODE:
        case NodeTypes.SUBSTRING_OPERATOR_NODE:
        case NodeTypes.TIMESTAMP_ADD_FN_NODE:
        case NodeTypes.TIMESTAMP_DIFF_FN_NODE:
            return ternaryOperatorFunction((TernaryOperatorNode)node);
        case NodeTypes.LEFT_FN_NODE:
        case NodeTypes.RIGHT_FN_NODE:
        case NodeTypes.TRIM_OPERATOR_NODE:
        case NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
        case NodeTypes.BINARY_MINUS_OPERATOR_NODE:
        case NodeTypes.BINARY_PLUS_OPERATOR_NODE:
        case NodeTypes.BINARY_TIMES_OPERATOR_NODE:
        case NodeTypes.MOD_OPERATOR_NODE:
        case NodeTypes.BINARY_BIT_OPERATOR_NODE:
            return binaryOperatorFunction((BinaryOperatorNode)node);
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
                                                    ArgumentsAccess args,
                                                    boolean isNullable)
            throws StandardException {
            return null;
/*
        ExpressionComposer composer;
        try {
            composer = functionsRegistry.composer(functionName);
        }
        catch (NoSuchFunctionException ex) {
            return null;        // Defer error until later.
        }
        int nargs = args.nargs();
        TypesList argTypes = new ArgTypesList(args);
        for (int i = 0; i < nargs; i++)
        {
            ExpressionType argType = args.argType(i);
            if (argType == null)
                return null;
            argTypes.add(argType);
        }
        ExpressionType resultType = composer.composeType(argTypes);

        if (resultType == null)
            return null;
        return fromExpressionType(resultType, isNullable);
*/        
    }


        // Compute type from function's composer with arguments' types.
    protected DataTypeDescriptor expressionComposer(String functionName,
                                                    ArgumentsAccess args )
            throws StandardException {
        return expressionComposer(functionName, args, true);
    }

    protected DataTypeDescriptor noArgFunction(String functionName) 
            throws StandardException {
        T3RegistryService.FunctionKind functionKind = 
            functionsRegistry.getFunctionKind(functionName);
        if (functionKind == T3RegistryService.FunctionKind.SCALAR)
            return expressionComposer(functionName,
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
        return null;
    }

    protected DataTypeDescriptor unaryOperatorFunction(UnaryOperatorNode node) 
            throws StandardException {
        return expressionComposer(node.getMethodName(), new UnaryValuesAccess(node));
    }

    protected DataTypeDescriptor binaryOperatorFunction(BinaryOperatorNode node) 
            throws StandardException {
        ValueNode leftOperand = node.getLeftOperand();
        ValueNode rightOperand = node.getRightOperand();
        DataTypeDescriptor leftType = leftOperand.getType();
        DataTypeDescriptor rightType = rightOperand.getType();
        if (isParameterOrUntypedNull(leftOperand) && (rightType != null))
            leftType = rightType.getNullabilityType(true);
        else if (isParameterOrUntypedNull(rightOperand) && (leftType != null)) 
            rightType = leftType.getNullabilityType(true);
        
        if ((leftType == null) || (rightType == null))
            return null;

        boolean nullable = leftType.isNullable() || rightType.isNullable();
        return expressionComposer(node.getMethodName(), new BinaryValuesAccess(node), nullable);
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
        private final int nargs;
        public TernaryValuesAccess(TernaryOperatorNode node) {
            this.node = node;
            nargs = node.getRightOperand() == null ? 2 : 3;
        }

        @Override
        public int nargs() {
            return nargs;
        }

        @Override
        public ValueNode argNode(int index) {
            switch (index) {
            case 0:
                return node.getReceiver();
            case 1:
                return node.getLeftOperand();
            case 2:
                if (nargs != 3)
                    throw new IndexOutOfBoundsException("Out of range index: " + index);
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
        if (methodCall.getUserData() != null) {
            Routine routine = (Routine)methodCall.getUserData();
            return ColumnBinding.getType(routine.getReturnValue());
        }
        if ((methodCall.getMethodParameters() == null) ||
            (methodCall.getMethodParameters().length == 0)) {
            return noArgFunction(methodCall.getMethodName());
        }
        else if (methodCall.getMethodParameters().length == 1) {
            return oneArgMethodCall(methodCall);
        }
        else {
            return expressionComposer(methodCall.getMethodName(),
                                      new JavaValuesAccess(methodCall.getMethodParameters()));
        }
    }

    protected DataTypeDescriptor oneArgMethodCall(MethodCallNode methodCall)
            throws StandardException {
        T3RegistryService.FunctionKind functionKind = 
            functionsRegistry.getFunctionKind(methodCall.getMethodName());
        if (functionKind == T3RegistryService.FunctionKind.SCALAR)
            return expressionComposer(methodCall.getMethodName(),
                                      new JavaValuesAccess(methodCall.getMethodParameters()));
        if (functionKind == T3RegistryService.FunctionKind.AGGREGATE) {
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
        case NodeTypes.CURRENT_SCHEMA_NODE:
            return "current_schema";
        case NodeTypes.CURRENT_ISOLATION_NODE:
        case NodeTypes.IDENTITY_VAL_NODE:
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

}
