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

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;
import org.slf4j.LoggerFactory;

public class UnaryBitExpression extends AbstractUnaryExpression
{
    public static enum UnaryBitOperator 
    {
        NOT
        {
            @Override
            public ValueSource exc (BigInteger arg)
            {
                return new ValueHolder(AkType.U_BIGINT, arg.not().and(n64));
            }
            
            @Override
            public ValueSource errorCase ()
            {
                return new ValueHolder(AkType.U_BIGINT,n64);
            }
        },
        
        COUNT
        {
            @Override
            public ValueSource exc (BigInteger arg)
            {
                return new ValueHolder(AkType.LONG, (long)arg.bitCount());
            }
            
            @Override
            public ValueSource errorCase ()
            {
                return new ValueHolder(AkType.LONG, 0L);
            }
        };

        protected abstract ValueSource exc (BigInteger arg);
        protected abstract ValueSource errorCase ();
        private static final BigInteger n64 = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)).add(BigInteger.ONE);
   }
    
    @Scalar("~")
    public static final ExpressionComposer NOT_COMPOSER = new InternalComposer(UnaryBitOperator.NOT);
    
    @Scalar("bit_count")
    public static final ExpressionComposer BIT_COUNT_COMPOSER = new InternalComposer(UnaryBitOperator.COUNT);
    
    private static class InternalComposer extends UnaryComposer
    {
        private final UnaryBitOperator op;
        public InternalComposer (UnaryBitOperator op)
        {
            this.op = op;
        }

        @Override
        protected Expression compose(Expression argument) 
        {
            return new UnaryBitExpression(argument, op);
        }
    }
            
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final UnaryBitOperator op;
        
        public InnerEvaluation (ExpressionEvaluation ev, UnaryBitOperator op)
        {
            super(ev);
            this.op = op;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource source;
            if ((source = this.operand()).isNull()) return NullValueSource.only();
            
            BigInteger arg;
            try
            {
                arg = Extractors.getUBigIntExtractor().getObject(source);
            }
            catch (InconvertibleTypesException ex) 
            {
                LoggerFactory.getLogger(UnaryBitExpression.class).debug(ex.getShortMessage() + " - assume 0 as input");
                return op.errorCase();
            } 
            catch (NumberFormatException ex)
            {
                LoggerFactory.getLogger(UnaryBitExpression.class).debug(ex.getMessage() + " - assume 0 as input");
                return op.errorCase();
            }           
            return op.exc(arg);            
        }        
    }
    
    private final UnaryBitOperator op;
    
    public UnaryBitExpression (Expression ex, UnaryBitOperator op)
    {
        super(op == UnaryBitOperator.COUNT ? AkType.LONG : AkType.U_BIGINT, ex);
        this.op = op;
    }

    @Override
    protected String name()
    {
        return op.name();
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(operandEvaluation(), op);
    }    
}
