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
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;
import org.joda.time.DateTime;

public class UnixTimestampExpression extends AbstractCompositeExpression
{
    @Scalar("unix_timestamp")
    public static final ExpressionComposer COMPOSER= new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {   
            switch(argumentTypes.size())
            {
                case 1:     argumentTypes.setType(0, AkType.TIMESTAMP); // fall thru;
                case 0:     break;
                default:    throw new WrongExpressionArityException(2, argumentTypes.size());
            }

            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new UnixTimestampExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    };

    @Override
    public String name() {
        return "TIMESTAMP";
    }
            
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            switch(children().size())
            {
                case 1:
                    ValueSource ts = children().get(0).eval();
                    if (ts.isNull())
                        return NullValueSource.only();
                    
                    long secs = ts.getTimestamp();
                    valueHolder().putLong(secs <= 0L ? 0L : secs);
                    break;
                case 0: // if called w/o argument, returns the current timestamp (similar to current_timestamp
                    valueHolder().putLong(new DateTime(queryContext().getCurrentDate()).getMillis() / 1000L);
                    break;
                default:
                    throw new WrongExpressionArityException(1, children().size());
            }
            return valueHolder();
        }
    }
    
    UnixTimestampExpression(List<? extends Expression> args)
    {
        super(AkType.LONG, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
