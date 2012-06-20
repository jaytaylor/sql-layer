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
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import com.akiban.util.ByteSource;
import com.akiban.util.Strings;
import com.akiban.util.WrappingByteSource;

public class UnhexExpression extends AbstractUnaryExpression
{
    @Scalar("unhex")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new UnhexExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);            
            return ExpressionTypes.varbinary(argumentTypes.get(0).getPrecision() / 2 + 1);
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final ByteSource EMPTY = new WrappingByteSource(new byte[0]);
        
        InnerEvaluation(ExpressionEvaluation arg)
        {
            super(arg);
        }
       
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
            
            String st = source.getString();
            if (st.isEmpty())
                valueHolder().putVarBinary(EMPTY);
            else
                try
                {
                    valueHolder().putVarBinary(Strings.parseHexWithout0x(st));
                }
                catch (InvalidOperationException e)
                {
                    QueryContext qc = queryContext();
                    if (qc != null)
                        qc.warnClient(e);
                    return NullValueSource.only();
                }
            
            return valueHolder();
        }
    }
    
    UnhexExpression (Expression arg)
    {
        super(AkType.VARBINARY, arg);
    }
    
    @Override
    public String name()
    {
        return "UNHEX";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
