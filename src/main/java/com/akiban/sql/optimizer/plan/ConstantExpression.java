/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;
import com.akiban.util.AkibanAppender;

/** An operand with a constant value. */
public class ConstantExpression extends BaseExpression 
{
    private Object value;
    private TPreptimeValue preptimeValue;

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
        // only store the preptimeValue if it's value is not null. If it's null, #value will just stay null.
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
