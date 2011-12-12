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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.util.List;

public class ReplaceExpression extends AbstractTernaryExpression
{
    @Scalar("replace")
    public static final ExpressionComposer COMPOSER = new TernaryComposer()
    {
        @Override
        protected Expression compose(Expression first, Expression second, Expression third)
        {
            return new ReplaceExpression(first, second, third);
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second, ExpressionType third)
        {
            return ExpressionTypes.varchar(first.getPrecision() + second.getPrecision() - third.getPrecision());
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            for (int n = 0; n < argumentTypes.size(); ++n)
                argumentTypes.set(n, AkType.VARCHAR);
        }
    };

    private static final class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource sources[] = getAll();
            String sts[] = new String[3];
            for (int n = 0; n < sources.length; ++n)
                if (sources[n].isNull()) return NullValueSource.only();
                else sts[n] = sources[n].getString();

            valueHolder().putRaw(AkType.VARCHAR,
                    sts[1].equals("") ? sts[0] : sts[0].replace(sts[1], sts[2]));
            return valueHolder();
                    
        }
    }

    public ReplaceExpression (Expression first, Expression second, Expression third)
    {
        super(AkType.VARCHAR, first, second, third);
    }

    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("REPLACE");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
