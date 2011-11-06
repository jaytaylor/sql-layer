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

package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.List;

/** A call to a function that returns something from the environment.
 */
public class EnvironmentFunctionExpression extends BaseExpression
{
    private String function;
    private int bindingPosition;
    
    public EnvironmentFunctionExpression(String function, int bindingPosition,
                                         DataTypeDescriptor sqlType, AkType akType, ValueNode sqlSource) {
        super(sqlType, akType, sqlSource);
        this.function = function;
        this.bindingPosition = bindingPosition;
    }
                              
    public String getFunction() {
        return function;
    }
    public int getBindingPosition() {
        return bindingPosition;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EnvironmentFunctionExpression)) return false;
        EnvironmentFunctionExpression other = (EnvironmentFunctionExpression)obj;
        return function.equals(other.function);
    }

    @Override
    public int hashCode() {
        return function.hashCode();
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        return v.visit(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(function);
        str.append("(");
        str.append(bindingPosition);
        str.append(")");
        return str.toString();
    }

}
