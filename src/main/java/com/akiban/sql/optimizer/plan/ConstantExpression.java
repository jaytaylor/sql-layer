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

import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.API;

import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An operand with a constant value. */
public class ConstantExpression extends BaseExpression 
{
    private Object value;

    public ConstantExpression(Object value, 
                              DataTypeDescriptor sqlType, AkType type, ValueNode sqlSource) {
        super(sqlType, type, sqlSource);
        if (value instanceof Integer)
            value = new Long(((Integer)value).intValue());
        this.value = value;
    }

    public ConstantExpression(Object value, DataTypeDescriptor sqlType, ValueNode sqlSource) {
        this(value, sqlType, FromObjectValueSource.reflectivelyGetAkType(value), sqlSource);
    }

    public ConstantExpression(ValueSource valueSource, DataTypeDescriptor sqlType, ValueNode sqlSource) {
        this(new ToObjectValueTarget().convertFromSource(valueSource), sqlType, valueSource.getConversionType(), sqlSource);
    }

    public ConstantExpression(Object value, AkType type) {
        this(value, null, type, null);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConstantExpression)) return false;
        ConstantExpression other = (ConstantExpression)obj;
        return ((value == null) ?
                (other.value == null) :
                value.equals(other.value));
    }

    @Override
    public int hashCode() {
        return (value == null) ? 0 : value.hashCode();
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
        return String.valueOf(value);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy object.
    }

}
