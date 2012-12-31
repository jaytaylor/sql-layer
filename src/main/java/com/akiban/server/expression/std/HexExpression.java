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
import java.nio.charset.Charset;

public class HexExpression extends AbstractUnaryExpression
{
    @Scalar("hex")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new HexExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            if (ArithExpression.isNumeric(argumentTypes.get(0).getType()))
                argumentTypes.setType(0, AkType.LONG);
            else
                argumentTypes.setType(0, AkType.VARCHAR);
            return ExpressionTypes.varchar(argumentTypes.get(0).getPrecision() * 2);
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private  Charset charset = Charset.defaultCharset(); // may change when actual charset
                                                          // becomes availabe to expressions
        
        public InnerEvaluation(ExpressionEvaluation eval)
        {
            super(eval);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
            
            if (source.getConversionType() == AkType.LONG)
                valueHolder().putString(Long.toHexString(source.getLong()).toUpperCase());
            else
            {
                StringBuilder builder = new StringBuilder();
                for (byte ch : source.getString().getBytes(charset))
                    builder.append(String.format("%02X", ch));
                valueHolder().putString(builder.toString());
            }
            
            return valueHolder();
        }
        
    }

    @Override
    public String name()
    {
        return "HEX";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
    
    protected HexExpression (Expression arg)
    {
        super(AkType.VARCHAR, arg);
    }
}
