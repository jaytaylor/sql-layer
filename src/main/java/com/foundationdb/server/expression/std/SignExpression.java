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

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InvalidArgumentTypeException;
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
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
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
                    if (Double.isNaN(operand().getDouble()))
                        return NullValueSource.only();
                    else
                        valueHolder().putInt(finalReturnValueOf(Double.compare(operand().getDouble(), 0.0d)));
                    break;
                case U_DOUBLE:
                    if (Double.isNaN(operand().getUDouble()))
                        return NullValueSource.only();
                    else
                        valueHolder().putInt(finalReturnValueOf(Double.compare(operand().getUDouble(), 0.0d))); 
                    break;
                case FLOAT:
                    if (Float.isNaN(operand().getFloat()))
                        return NullValueSource.only();
                    else
                        valueHolder().putInt(finalReturnValueOf(Float.compare(operand().getFloat(), 0.0f))); 
                    break;
                case U_FLOAT:
                    if (Float.isNaN(operand().getUFloat()))
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
                case VARCHAR:
                    double parsedStrInput = Double.parseDouble(operand().getString());
                    valueHolder().putInt(finalReturnValueOf( Double.compare(parsedStrInput, 0.0d)));
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
    public String name()
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
