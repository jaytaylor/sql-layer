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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;
import java.math.BigDecimal;
import java.util.List;

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
    public Explainer getExplainer()
    {
        Explainer ex = new ExpressionExplainer(Type.FUNCTION, name(), (List)null);
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(valueSource));
        return ex;
    }


    private class InnerEvaluation implements ExpressionEvaluation
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
