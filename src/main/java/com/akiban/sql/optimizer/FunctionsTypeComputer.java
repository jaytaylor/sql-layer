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

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionRegistry;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.ExpressionTypes;

import com.akiban.server.types.AkType;

import com.akiban.server.error.NoSuchFunctionException;

import java.util.ArrayList;
import java.util.List;

/** Calculate types from expression composers. */
public class FunctionsTypeComputer extends AISTypeComputer
{
    private ExpressionRegistry expressionRegistry;

    public FunctionsTypeComputer(ExpressionRegistry expressionRegistry) {
        this.expressionRegistry = expressionRegistry;
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
        else if (javaValue instanceof SQLToJavaValueNode) {
            return computeType(((SQLToJavaValueNode)javaValue).getSQLValueNode());
        }
        else {
            return null;
        }
    }

    protected DataTypeDescriptor methodCallNode(MethodCallNode methodCall)
            throws StandardException {
        ExpressionComposer composer;
        try {
            composer = expressionRegistry.composer(methodCall.getMethodName());
        }
        catch (NoSuchFunctionException ex) {
            return null;
        }
        List<ExpressionType> argumentTypes = new ArrayList<ExpressionType>();
        if (methodCall.getMethodParameters() != null) {
            for (JavaValueNode javaValue : methodCall.getMethodParameters()) {
                argumentTypes.add(toExpressionType(javaValue.getType()));
            }
        }
        ExpressionType resultType = composer.composeType(argumentTypes);
        if (resultType == null)
            return null;
        return fromExpressionType(resultType);
    }

    /* Yet another translator between type regimes. */

    protected ExpressionType toExpressionType(DataTypeDescriptor sqlType) {
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
            {
                final AkType type = AkType.valueOf(sqlType.getFullSQLTypeName().toUpperCase());
                final int precision = sqlType.getPrecision();
                final int scale = sqlType.getScale();
                return new ExpressionType() {
                        @Override
                        public AkType getType() {
                            return type;
                        }

                        @Override
                        public int getPrecision() {
                            return precision;
                        }

                        @Override
                        public int getScale() {
                            return scale;
                        }
                    };
            }
        default:
            return null;
        }
    }

    protected DataTypeDescriptor fromExpressionType(ExpressionType resultType) {
        switch (resultType.getType()) {
        case BOOL:
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
        case INT:
        case YEAR:
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
        case DATETIME:
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
        default:
            return null;
        }
    }

}
