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

package com.akiban.sql.optimizer.query;

import com.akiban.sql.StandardException;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.expression.Expression;

import java.util.List;

public class FunctionCallExpression extends BaseExpression 
{
    private BaseExpression operand;
    private String function;
    private List<BaseExpression> additionalOperands;
    
    public FunctionCallExpression(BaseExpression operand, String function,
                                  List<BaseExpression> additionalOperands,
                                  DataTypeDescriptor type) {
        super(type);
        this.operand = operand;
        this.function = function;
        this.additionalOperands = additionalOperands;
    }

    public BaseExpression getOperand() {
        return operand;
    }
    public String getFunction() {
        return function;
    }
    public List<BaseExpression> getAdditionalOperands() {
        return additionalOperands;
    }

    public String toString() {
        StringBuilder str = new StringBuilder(function);
        str.append("(");
        str.append(operand);
        if (additionalOperands != null) {
            for (BaseExpression additionalOperand : additionalOperands) {
                str.append(",");
                str.append(additionalOperand);
            }
        }
        str.append(")");
        return str.toString();
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) 
            throws StandardException {
        throw new StandardException("NIY");
    }

}
