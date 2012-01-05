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
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import org.slf4j.LoggerFactory;

public class BitLengthExpression extends AbstractUnaryExpression
{
    @Scalar("bit_length")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        // TODO: need a new method (probably in the ExpressionComposer)
        // that would take an extra argument (VARCHAR) which contains
        // the name of the CHARSET.
        // For now compose(...) cannot take such argument

        @Override
        protected Expression compose(Expression argument)
        {
            return new BitLengthExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
           if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
           argumentTypes.setType(0, AkType.VARCHAR);
           return ExpressionTypes.LONG;
        }
    };

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final String CHARSET;
        public InnerEvaluation (ExpressionEvaluation operand, String charset)
        {
            super(operand);
            this.CHARSET = charset;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
             else
                if (CHARSET != null)
                    try
                    {
                        return new ValueHolder(AkType.LONG, source.getString().getBytes(CHARSET).length * 8);
                    }
                    catch (UnsupportedEncodingException ex)
                    {
                        LoggerFactory.getLogger(BitLengthExpression.class).error("Un-recognised CHARSET", ex);
                        return NullValueSource.only();
                    }
                else
                    return new ValueHolder(AkType.LONG, source.getString().getBytes(Charset.defaultCharset()).length * 8);
        }
    }

    public BitLengthExpression (Expression arg)
    {
        super(AkType.LONG, arg);
        CHARSET = null;
    }

    public BitLengthExpression (Expression arg, String charset)
    {
        super(AkType.LONG, arg);
        this.CHARSET = charset;
    }
    
    @Override
    protected String name()
    {
        return "BIT_LENGTH";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), CHARSET);
    }

    private final String CHARSET;
}
