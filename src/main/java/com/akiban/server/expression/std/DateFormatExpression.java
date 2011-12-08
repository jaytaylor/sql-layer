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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.util.List;
import org.joda.time.MutableDateTime;

public class DateFormatExpression extends AbstractBinaryExpression
{
    @Scalar("date_format")
    public static final ExpressionComposer COMPOSER = new BinaryComposer ()
    {
        @Override
        protected Expression compose(Expression first, Expression second)
        {
            return new DateFormatExpression(first, second);
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second)
        {
            return ExpressionTypes.varchar(second.getScale() * 4);
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            argumentTypes.set(1, AkType.VARCHAR);
            AkType dateType = argumentTypes.get(0);
            if (dateType != AkType.DATE && dateType != AkType.DATETIME
                    && dateType != AkType.TIME && dateType != AkType.TIMESTAMP
                    && dateType != AkType.YEAR)
                argumentTypes.set(0, AkType.DATE);
        }
    };
    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource date = children().get(0).eval();
            ValueSource format = children().get(1).eval();
            if (date.isNull() || format.isNull()) return NullValueSource.only();
            MutableDateTime datetime = (MutableDateTime)Extractors.getObjectExtractor(AkType.DATE).getObject(date);
            
            return new ValueHolder(AkType.VARCHAR, DateTimeField.getFormatted(datetime, format.getString()));
        }        
    }
    
    protected DateFormatExpression (Expression left, Expression right)
    {
        super(AkType.VARCHAR, left, right);
    }
    
    @Override
    protected boolean nullIsContaminating() 
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append("DATE_FORMAT");
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluations());
    }    
}
