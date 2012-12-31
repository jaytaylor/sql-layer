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
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;

public class AbsExpression extends AbstractUnaryExpression 
{    
    @Scalar ({"absolute", "abs"})
    public static final ExpressionComposer COMPOSER = new InternalComposer();
    
    private static class InternalComposer extends UnaryComposer
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new AbsExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1) 
                throw new WrongExpressionArityException(1, argumentTypes.size());
           
            ExpressionType argExpType = argumentTypes.get(0);
            AkType argAkType = argExpType.getType();
            
            // Cast both VARCHAR and UNSUPPORTED; UNSUPPORTED appearing on SQL params (ABS(?) query)
            if (argAkType == AkType.VARCHAR || argAkType == AkType.UNSUPPORTED)
            {
                argumentTypes.setType(0, AkType.DOUBLE);
            }
            
            return argumentTypes.get(0);
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
            
            switch (operandType) {
                case VARCHAR:
                    valueHolder().putDouble( Math.abs(Double.parseDouble(operand().getString())));
                    break;
                case DOUBLE:
                    valueHolder().putDouble( Math.abs(operand().getDouble()) ); 
                    break;   
                case FLOAT:
                    valueHolder().putFloat( Math.abs(operand().getFloat()) ); 
                    break;
                case LONG:
                    valueHolder().putLong( Math.abs(operand().getLong()) ); 
                    break;
                case INT:
                    valueHolder().putInt( Math.abs(operand().getInt()) ); 
                    break;
                case DECIMAL:
                    valueHolder().putDecimal( operand().getDecimal().abs()); 
                    break;
                case U_DOUBLE: 
                case U_BIGINT: 
                case U_FLOAT: 
                case U_INT:
                    // Unsigned values remain the same
                    valueHolder().copyFrom(operand()); 
                    break;
                default:
                    QueryContext context = queryContext();
                    if (context != null)
                        context.warnClient(new InvalidArgumentTypeException("ABS: " + operandType.name()));
                    return NullValueSource.only();
            }
            
            return valueHolder();
        }  
        
    }
    
    protected AbsExpression(Expression operand)
    {
        // ctor sets type and value
        super(operand.valueType(), operand);
    }
    
    @Override
    public String name() 
    {
        return "ABS";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(this.operandEvaluation());
    }
    
}
