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
import com.foundationdb.server.expression.*;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.Arrays;
import java.util.List;

public class MakeTimeExpression extends AbstractTernaryExpression
{
    @Scalar("maketime")
    public static final ExpressionComposer COMPOSER = new TernaryComposer()
    {
        
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            for (int n = 0; n < argumentTypes.size(); n++) 
            {
                argumentTypes.setType(n, AkType.LONG);
            }
            return ExpressionTypes.TIME;
            
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new MakeTimeExpression(arguments);
        }

        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new MakeTimeExpression(arguments);
        }
        
    };

    @Override
    public String name() {
        return "MAKETIME";
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
            for (int n = 0; n < sources.length; n++) 
            {
                if (sources[n].isNull()) return NullValueSource.only();
            }
            // Time input format HHMMSS
            long hours = sources[0].getLong();
            long minutes = sources[1].getLong();
            long seconds = sources[2].getLong();
            
            // Check for valid input
            if (minutes >= 60 || minutes < 0) return NullValueSource.only();
            if (seconds >= 60 || seconds < 0) return NullValueSource.only();
            
            long time = hours < 0 ? -1 : 1;
            hours *= time;
            time *= seconds + minutes * 100 + hours * 10000;
            valueHolder().putTime(time);
            return valueHolder();
        }
                
    }
    
    public MakeTimeExpression (List<? extends Expression> args)
    {
        super(AkType.TIME, args);
    }

    // for testing
    MakeTimeExpression (Expression arg1, Expression arg2, Expression arg3)
    {
        this(Arrays.asList(arg1, arg2, arg3));
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
