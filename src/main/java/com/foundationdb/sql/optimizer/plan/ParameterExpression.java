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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/** An operand with a parameter value. */
public class ParameterExpression extends BaseExpression 
{
    private int position;
    private Object value;
    private boolean isSet;

    public ParameterExpression(int position, 
                               DataTypeDescriptor sqlType, ValueNode sqlSource,
                               TInstance type) {
        super(sqlType, sqlSource, type);
        this.position = position;
        this.value = null;
        this.isSet = false;
    }

    public ParameterExpression(int position, 
            DataTypeDescriptor sqlType, ValueNode sqlSource,
            TInstance type, Object value) {
        super(sqlType, sqlSource, type);
        this.position = position;
        this.value = value;
        this.isSet = true;
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

    public void setValue(Object value) {
        this.value = value;
        isSet = true;
    }

    public Object getValue() {
        return value;
    }

    public boolean isSet() {
        return isSet;
    }
}
