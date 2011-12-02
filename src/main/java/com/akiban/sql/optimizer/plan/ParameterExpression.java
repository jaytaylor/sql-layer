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

/** An operand with a parameter value. */
public class ParameterExpression extends BaseExpression 
{
    private int position;

    public ParameterExpression(int position, 
                               DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.position = position;
    }

    public ParameterExpression(int position, 
                               DataTypeDescriptor sqlType, AkType akType, 
                               ValueNode sqlSource) {
        super(sqlType, akType, sqlSource);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ParameterExpression)) return false;
        ParameterExpression other = (ParameterExpression)obj;
        return (position == other.position);
    }

    @Override
    public int hashCode() {
        return position;
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
        return "$" + position;
    }

}
