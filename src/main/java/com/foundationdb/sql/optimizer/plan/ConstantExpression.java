/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ToObjectValueTarget;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;
import com.foundationdb.sql.optimizer.TypesTranslation;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.util.AkibanAppender;

/** An operand with a constant value. */
public class ConstantExpression extends BaseExpression 
{
    private Object value;
    private TPreptimeValue preptimeValue;

    public static ConstantExpression typedNull(DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance tInstance) {
        ConstantExpression result = new ConstantExpression(null, sqlType, AkType.NULL, sqlSource);
        PValueSource nullSource = PValueSources.getNullSource(tInstance);
        result.preptimeValue = new TPreptimeValue(tInstance, nullSource);
        return result;
    }

    public ConstantExpression(Object value, 
                              DataTypeDescriptor sqlType, AkType type, ValueNode sqlSource) {
        super(sqlType, type, sqlSource);
        this.value = value;
    }

    public ConstantExpression(Object value, DataTypeDescriptor sqlType, ValueNode sqlSource) {
        this(value, sqlType, FromObjectValueSource.reflectivelyGetAkType(value), sqlSource);
    }

    public ConstantExpression(ValueSource valueSource, DataTypeDescriptor sqlType, ValueNode sqlSource) {
        this(
                valueSource.isNull() ? null : new ToObjectValueTarget().convertFromSource(valueSource),
                sqlType,
                valueSource.getConversionType(),
                sqlSource
        );
    }

    public ConstantExpression(Object value, AkType type) {
        this(value, null, type, null);
    }

    public ConstantExpression(TPreptimeValue preptimeValue) {
        this(preptimeValue.instance().dataTypeDescriptor());
        // only store the preptimeValue if its value is not null. If it's null, #value will just stay null.
        this.preptimeValue = preptimeValue;
    }

    private ConstantExpression(DataTypeDescriptor sqlType) {
        this(null, sqlType, TypesTranslation.sqlTypeToAkType(sqlType), null);
    }

    @Override
    public TPreptimeValue getPreptimeValue() {
        if (preptimeValue == null)
            preptimeValue = PValueSources.fromObject(value, getAkType());
        return preptimeValue;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public Object getValue() {
        if (value == null && preptimeValue != null) {
            PValueSource pValueSource = preptimeValue.value();
            if (pValueSource == null || pValueSource.isNull())
                return null;
            value = PValueSources.toObject(pValueSource, getAkType());
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

    private static void ensureValueObject(ConstantExpression constantExpression) {
        if ( (constantExpression.value == null) && (constantExpression.preptimeValue != null) )
            constantExpression.getValue();
    }

    @Override
    public int hashCode() {
        ensureValueObject(this);
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
        if (preptimeValue != null) {
            PValueSource valueSource = preptimeValue.value();
            if (valueSource == null || valueSource.isNull())
                return "NULL";
            TInstance tInstance = preptimeValue.instance();
            StringBuilder sb = new StringBuilder();
            tInstance.format(valueSource, AkibanAppender.of(sb));
            return sb.toString();
        }
        ValueSource valueSource;
        if (getAkType() == null)
            valueSource = new FromObjectValueSource().setReflectively(value);
        else
            valueSource = new FromObjectValueSource().setExplicitly(value, getAkType());
        // TODO: ValueHolder seems to be the only ValueSource that prints itself well.
        valueSource = new ValueHolder(valueSource);
        return valueSource.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy object.
    }

}
