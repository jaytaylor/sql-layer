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
import com.foundationdb.server.types.util.ValueSources;
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;
import java.util.Arrays;
import java.util.List;

/** Note: This isn't the <code>MAX</code> aggregate function, but its scalar cousin. */
public class MinMaxExpression extends AbstractBinaryExpression
{

    @Override
    public String name() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public static enum Operation { MIN, MAX }
    
    private final Operation operation;
    
    @Scalar ("_min")
    public static final ExpressionComposer MIN_COMPOSER = new InternalComposer(Operation.MIN);
    
    @Scalar ("_max")
    public static final ExpressionComposer MAX_COMPOSER = new InternalComposer(Operation.MAX);
    
    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        final Operation operation;
        
        public InnerEvaluation (List< ? extends ExpressionEvaluation> children, Operation operation)
        {
            super(children);
            this.operation = operation;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource v1 = left();
            if (v1.isNull()) return NullValueSource.only();
            
            ValueSource v2 = right();
            if (v2.isNull()) return NullValueSource.only();
            
            return (((ValueSources.compare(v1, v2) > 0) == (operation == Operation.MAX)) ? v1 : v2);
        }
    }
    
    private static final class InternalComposer extends BinaryComposer
    {
        private final Operation operation;
        
        public InternalComposer (Operation operation)
        {
            this.operation = operation;
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            AkType topType = CoalesceExpression.getTopType(Arrays.asList(argumentTypes.get(0).getType(), 
                                                                         argumentTypes.get(1).getType()));
            argumentTypes.setType(0, topType);
            argumentTypes.setType(1, topType);
            return ExpressionTypes.newType(topType,
                                           Math.max(argumentTypes.get(0).getPrecision(), argumentTypes.get(1).getPrecision()),
                                           Math.max(argumentTypes.get(0).getScale(), argumentTypes.get(1).getScale()));
        }

        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new MinMaxExpression(first, second, operation);
        }
    }
  
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(operation);
    }
    
    public MinMaxExpression(Expression first, Expression second, Operation operation)
    {
        super(CoalesceExpression.getTopType(Arrays.asList(first.valueType(), second.valueType())), 
              first, second);
        this.operation = operation;
    }
    
    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluations(), operation);
    }    
}
