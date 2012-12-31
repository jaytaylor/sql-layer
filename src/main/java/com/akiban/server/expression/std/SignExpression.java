/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
