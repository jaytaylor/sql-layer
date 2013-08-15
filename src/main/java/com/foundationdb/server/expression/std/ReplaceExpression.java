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

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;
import java.util.List;

public class ReplaceExpression extends AbstractTernaryExpression
{
    @Scalar("replace")
    public static final ExpressionComposer COMPOSER = new TernaryComposer()
    {
        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ReplaceExpression(arguments);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            int length = 0;
            for (int n  = 0; n < argumentTypes.size(); ++n)
            {
                argumentTypes.setType(n, AkType.VARCHAR);
                length += argumentTypes.get(n).getPrecision();
            }
            return ExpressionTypes.varchar(length);
        }
    };

    @Override
    public String name()
    {
        return "REPLACE";
    }

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

    public ReplaceExpression (List<? extends Expression> arguments)
    {
        super(AkType.VARCHAR, arguments);
    }

    @Override
    public boolean nullIsContaminating()
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
