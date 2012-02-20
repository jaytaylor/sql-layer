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
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.util.ConversionUtil;
import com.akiban.sql.StandardException;
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
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());

            argumentTypes.setType(1, AkType.VARCHAR);
            ExpressionType dateType = argumentTypes.get(0);
            switch(dateType.getType())
            {
                case DATE:
                case TIME:
                case DATETIME:
                case TIMESTAMP: break;
                case VARCHAR:   argumentTypes.setType(0, dateType.getPrecision() > 10 ?
                                                     AkType.DATETIME: AkType.DATE);
                                break;
                default:        argumentTypes.setType(0, AkType.DATE);
            }            

            return ExpressionTypes.varchar(argumentTypes.get(1).getPrecision() * 5);
        }
    };

    @Override
    public String name()
    {
        return "DATE_FORMAT";
    }
    
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
            if (date.isNull() || format.isNull() || format.getString().equals("")) return NullValueSource.only();
            MutableDateTime datetime;
            try
            {
                datetime = ConversionUtil.getDateTimeConverter().get(date);
            }
            catch (InvalidParameterValueException ex)
            {
                QueryContext context = queryContext();
                if (context != null)
                    context.warnClient(ex);
                return NullValueSource.only();
            }
            valueHolder().putString(DateTimeField.getFormatted(datetime, format.getString()));
            return valueHolder();
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
