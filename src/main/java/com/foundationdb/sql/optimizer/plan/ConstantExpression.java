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

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.optimizer.TypesTranslation;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.util.AkibanAppender;

/** An operand with a constant value. */
public class ConstantExpression extends BaseExpression 
{
    private Object value;
    // TODO: Remove this. It hides a preptimeValue in BaseExpression. 
    // But they are used differently somewhere and RulesTest fails
    // if you do the simple removal. 
    private TPreptimeValue preptimeValue;

    public static ConstantExpression typedNull(DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance tInstance) {
        if (sqlType == null) {
            ValueSource nullSource = ValueSources.getNullSource(null);
            ConstantExpression result = new ConstantExpression(new TPreptimeValue(null, nullSource));
            return result;
        }
        ConstantExpression result = new ConstantExpression((Object)null, sqlType, sqlSource);
        if (tInstance != null) {
            ValueSource nullSource = ValueSources.getNullSource(tInstance);
            result.setPreptimeValue(new TPreptimeValue(tInstance, nullSource));
        } else {
            result.setPreptimeValue(new TPreptimeValue());
        }
        return result;
    }

    public ConstantExpression (Object value, DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super (sqlType, sqlSource);
        this.value = value;
        TInstance tInstance = TypesTranslation.toTInstance(sqlType);
        
        // For Constant Expressions, reset the CollationID to Null, meaning for
        // Constants the strings defer to column collation ordering. 
        if (tInstance != null && tInstance.typeClass() == MString.VARCHAR) {
            tInstance = MString.VARCHAR.instance(
                   tInstance.attribute(StringAttribute.MAX_LENGTH), 
                   tInstance.attribute(StringAttribute.CHARSET),
                   StringFactory.NULL_COLLATION_ID, 
                   tInstance.nullability());
        }
        this.preptimeValue = ValueSources.fromObject(value, tInstance);
    }
   
    public ConstantExpression(TPreptimeValue preptimeValue) {
        super (preptimeValue.instance() == null ? null : preptimeValue.instance().dataTypeDescriptor(), null);
        this.value = null; 
        this.preptimeValue = preptimeValue;
    }

    @Override
    public TPreptimeValue getPreptimeValue() {
        if (preptimeValue == null) {
            this.preptimeValue = ValueSources.fromObject(value, (TInstance) null);
        }
        return preptimeValue;
    }

    @Override
    public boolean isConstant() {
        return true;
    }
    
    public boolean isNumeric() {
        if (value != null) {
            if ((value instanceof Long) || (value instanceof Integer) ||
                    (value instanceof Short) || (value instanceof Byte)) {
                return true;
            }
        } else {
            TPreptimeValue v = preptimeValue;
            if (v != null) {
                TClass tclass = v.instance().typeClass();
                if (tclass == MNumeric.SMALLINT || tclass == MNumeric.MEDIUMINT ||
                    tclass == MNumeric.INT || tclass == MNumeric.BIGINT) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public boolean isNullable() {
        if (value == null && preptimeValue != null && preptimeValue.instance() != null) {
            return  preptimeValue.instance().nullability();
        }
        return false;
    }

    public Object getValue() {
        if (value == null && preptimeValue != null) {
            ValueSource valueSource = preptimeValue.value();
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

    private static void ensureValueObject(ConstantExpression constantExpression) {
        if ( (constantExpression.value == null) && (constantExpression.getPreptimeValue() != null) )
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
        ValueSource valueSource;
        TInstance tInstance;

        if (preptimeValue != null) {
            valueSource = preptimeValue.value();
            tInstance = preptimeValue.instance();
        } else {
            valueSource = ValueSources.fromObject(value, (TInstance) null).value();
            tInstance = (valueSource == null? null : valueSource.tInstance());
        }
        if (valueSource == null || valueSource.isNull())
            return "NULL";
        StringBuilder sb = new StringBuilder();
        tInstance.format(valueSource, AkibanAppender.of(sb));
        return sb.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy object.
    }

}
