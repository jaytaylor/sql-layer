/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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
import com.akiban.server.error.InvalidArgumentTypeException;
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
import com.akiban.sql.StandardException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class SignExpression extends AbstractUnaryExpression 
{
    public static final int NEG = -1, ZERO = 0, POS = 1;
    
    // This function takes the result from a `compare` function and maps it to
    // the desired return for SIGN. also allows us to change -1, 1, 0 if we want
    private static int finalReturnValueOf(int x)
    {
        if (x < 0) return NEG;
        if (x > 0) return POS;
        return ZERO;
    }
    
    @Scalar("sign")
    public static final ExpressionComposer COMPOSER = new InternalComposer();
    
    private static class InternalComposer extends UnaryComposer
    {
        @Override
        protected Expression compose(Expression argument)
        {
            return new SignExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());

            if (argumentTypes.get(0).getType() == AkType.VARCHAR)
                argumentTypes.setType(0, AkType.DOUBLE);
            
            return ExpressionTypes.INT;
        }
        
    }
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        public InnerEvaluation(ExpressionEvaluation eval)
        {
            super(eval);
        }
        
        @Override
        public ValueSource eval()
        {
            if (operand().isNull())
                return NullValueSource.only();
            
            AkType operandType = operand().getConversionType();
            switch (operandType)
            {
                // If the input is NaN, return NULL for type simplicity
                case DOUBLE:
                    if ((new Double(operand().getDouble())).equals(new Double(Double.NaN)))
                        return NullValueSource.only();
                    else
                        valueHolder().putInt(finalReturnValueOf(Double.compare(operand().getDouble(), 0.0d)));
                    break;
                case U_DOUBLE:
                    if ((new Double(operand().getUDouble())).equals(new Double(Double.NaN)))
                        return NullValueSource.only();
                    else
                        valueHolder().putInt(finalReturnValueOf(Double.compare(operand().getUDouble(), 0.0d))); 
                    break;
                case FLOAT:
                    if ((new Float(operand().getFloat())).equals(new Float(Float.NaN)))
                        return NullValueSource.only();
                    else
                        valueHolder().putInt(finalReturnValueOf(Float.compare(operand().getFloat(), 0.0f))); 
                    break;
                case U_FLOAT:
                    if ((new Float(operand().getUFloat())).equals(new Float(Float.NaN)))
                        return NullValueSource.only();
                    else                    
                        valueHolder().putInt(finalReturnValueOf(Float.compare(operand().getUFloat(), 0.0f))); 
                    break;
                case LONG:
                    Long longInput = new Long(operand().getLong());
                    valueHolder().putInt(finalReturnValueOf(longInput.compareTo(new Long(0L) ))); 
                    break;
                case INT:
                    Long intInput = new Long(operand().getInt());
                    valueHolder().putInt(finalReturnValueOf(intInput.compareTo(new Long(0L) ))); 
                    break;
                case U_INT:
                    Long u_intInput = new Long(operand().getUInt());
                    valueHolder().putInt(finalReturnValueOf(u_intInput.compareTo(new Long(0L) ))); 
                    break;
                case DECIMAL:
                    valueHolder().putInt(finalReturnValueOf( operand().getDecimal().compareTo(BigDecimal.ZERO) )); 
                    break;
                case U_BIGINT:                    
                    valueHolder().putInt(finalReturnValueOf( operand().getUBigInt().compareTo(BigInteger.ZERO) )); 
                    break;
                default:
                    QueryContext context = queryContext();
                    if (context != null)
                        context.warnClient(new InvalidArgumentTypeException("SIGN: " + operandType.name()));
                    return NullValueSource.only();
            }

            return valueHolder();
        }
        
    }
    
    @Override
    protected String name()
    {
        return "sign";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(this.operandEvaluation());
    }
    
    protected SignExpression(Expression operand)
    {
        super(AkType.INT, operand);
    }

}
