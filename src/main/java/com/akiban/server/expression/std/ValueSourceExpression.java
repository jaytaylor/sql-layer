
package com.akiban.server.expression.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.ExpressionExplainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.SqlLiteralValueFormatter;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public final class ValueSourceExpression implements Expression
{
    // Expression interface

    @Override
    public boolean isConstant()
    {
        return false;
    }

    @Override
    public boolean needsBindings()
    {
        return false;
    }

    @Override
    public boolean needsRow()
    {
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation();
    }

    @Override
    public AkType valueType()
    {
        return valueSource.getConversionType();
    }

    public ValueSourceExpression(ValueSource valueSource)
    {
        this.valueSource = valueSource;
    }

    // Object interface

    @Override
    public String toString()
    {
        return String.format("ValueSource(%s)", valueSource);
    }

    // object state

    private final ValueSource valueSource;

    @Override
    public String name()
    {
        return "VALUESOURCE";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.FUNCTION, name(), context);
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(SqlLiteralValueFormatter.format(valueSource)));
        return ex;
    }

    public boolean nullIsContaminating()
    {
        return true;
    }
    
    private class InnerEvaluation extends ExpressionEvaluation.Base
    {
        @Override
        public void of(Row row)
        {
        }

        @Override
        public void of(QueryContext context)
        {
        }

        @Override
        public ValueSource eval()
        {
            return valueSource;
        }

        @Override
        public void acquire()
        {
            ++ownedBy;
        }

        @Override
        public boolean isShared()
        {
            return ownedBy > 1;
        }

        @Override
        public void release()
        {
            assert ownedBy > 0 : ownedBy;
            --ownedBy;
        }

        private int ownedBy = 0;
    }
}
