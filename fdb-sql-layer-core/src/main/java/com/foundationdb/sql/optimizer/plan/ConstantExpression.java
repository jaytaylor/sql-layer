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
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.util.AkibanAppender;

/** An operand with a constant value. */
public class ConstantExpression extends BaseExpression 
{
    private Object value;

    public static ConstantExpression typedNull(DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance type) {
        if (sqlType == null) {
            // TInstance.LiteralNull
            ValueSource nullSource = ValueSources.getNullSource(null);
            return new ConstantExpression(new TPreptimeValue(nullSource));
        }
        if (type != null) {
            ValueSource nullSource = ValueSources.getNullSource(type);
            return new ConstantExpression((Object)null, sqlType, sqlSource, new TPreptimeValue(type, nullSource));
        } else {
            // TInstance.LiteralNull
            return new ConstantExpression((Object)null, sqlType, sqlSource, new TPreptimeValue());
        }
    }

    public ConstantExpression (Object value, DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance type) {
        this (value, sqlType, sqlSource, getPreptimeValue(value, type));
    }
    public ConstantExpression (Object value, DataTypeDescriptor sqlType, ValueNode sqlSource, TPreptimeValue preptimeValue) {
        super (sqlType, sqlSource, preptimeValue);
        this.value = value;
    }

    public ConstantExpression (Object value, TInstance type) {
        this(value, type.dataTypeDescriptor(), null, type);
    }

    public ConstantExpression(TPreptimeValue preptimeValue) {
        super (preptimeValue.type() == null ? null : preptimeValue.type().dataTypeDescriptor(), null, preptimeValue);
    }

    private static TPreptimeValue getPreptimeValue(Object value, TInstance type) {
        // For constant strings, reset the CollationID to NULL,
        // meaning they defer collation ordering to the other operand.
        if (type != null && type.typeClass() instanceof TString) {
            type = type.typeClass().instance(
                    type.attribute(StringAttribute.MAX_LENGTH),
                    type.attribute(StringAttribute.CHARSET),
                    StringFactory.NULL_COLLATION_ID,
                    type.nullability());
        }

        //TODO ensure type is not null ever
        return ValueSources.fromObject(value, type);
    }

    @Override
    public boolean isConstant() {
        return true;
    }
    
    public boolean isNullable() {
        if (value == null && getType() != null) {
            return getType().nullability();
        }
        return false;
    }

    public Object getValue() {
        if (value == null) {
            ValueSource valueSource = getPreptimeValue().value();
            if (valueSource == null || valueSource.isNull())
                return null;
            value = ValueSources.toObject(valueSource);
        }
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConstantExpression)) return false;
        ConstantExpression other = (ConstantExpression)obj;
        // Normalize to the value (from TPreptimeContext)
        ensureValueObject(this);
        ensureValueObject(other);
        return ((value == null) ?
                (other.value == null) :
                value.equals(other.value));
    }

    @Override
    public int hashCode() {
        ensureValueObject(this);
        return (value == null) ? 0 : value.hashCode();
    }

    private static void ensureValueObject(ConstantExpression constantExpression) {
        if (constantExpression.value == null)
            constantExpression.getValue();
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
        ValueSource valueSource = getPreptimeValue().value();
        if (valueSource == null || valueSource.isNull())
            return "NULL";

        StringBuilder sb = new StringBuilder();
        getType().format(valueSource, AkibanAppender.of(sb));
        return sb.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy object.
    }

}
