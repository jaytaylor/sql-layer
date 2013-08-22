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

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

public class IfExpression extends AbstractCompositeExpression
{
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("IF()");
    }

    @Scalar("if")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size != 3)
                throw new WrongExpressionArityException(3, size);
            else
            {
                argumentTypes.setType(0, AkType.BOOL);
                AkType topType = CoalesceExpression.getTopType(Arrays.asList(argumentTypes.get(1).getType(), 
                                                                             argumentTypes.get(2).getType()));
                argumentTypes.setType(1, topType);
                argumentTypes.setType(2, topType);

                return ExpressionTypes.newType(topType,
                        Math.max(argumentTypes.get(1).getPrecision(), argumentTypes.get(2).getPrecision()),
                        Math.max(argumentTypes.get(1).getScale(), argumentTypes.get(2).getScale()));
            }

        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new IfExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
    };
    protected static final EnumSet<AkType> STRING = EnumSet.of(AkType.VARCHAR, AkType.TEXT);

    protected static AkType checkArgs(List<? extends Expression> children)
    {
        if (children.size() != 3)
            throw new WrongExpressionArityException(3, children.size());
        else
            return CoalesceExpression.getTopType(Arrays.asList(children.get(1).valueType(),
                                                               children.get(2).valueType()));
    }

    @Override
    public String name()
    {
        return "IF";
    }

    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> eva)
        {
            super(eva);
        }

        @Override
        public ValueSource eval()
        {
            return children().get(Extractors.getBooleanExtractor().getBoolean(children().get(0).eval(), false).booleanValue() ? 1 : 2).eval();
        }
    }
    
    public IfExpression(List<? extends Expression> children)
    {
        super(checkArgs(children), children);
    }

    @Override
    public boolean nullIsContaminating()
    {
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
