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
import java.util.List;


public class LeftRightExpression extends AbstractBinaryExpression
{
    @Override
    public String name() {
        return op.name();
    }
    protected enum Op
    {
        LEFT
        {
            @Override
            String getSubstring(String st, int length)
            {
                 return st.substring(0, length);
            }
        },
        RIGHT
        {
            @Override
            String getSubstring(String st, int length)
            {
                 return st.substring(st.length() - length, st.length());
            }
        };
        
        abstract String getSubstring(String st, int length);
    }
    
    @Scalar("getLeft")
    public static final ExpressionComposer LEFT_COMPOSER = new InnerComposer(Op.LEFT);
    
    @Scalar("getRight")
    public static final ExpressionComposer RIGHT_COMPOSER = new InnerComposer(Op.RIGHT);
    
    private static class InnerComposer extends BinaryComposer
    {
        private final Op op;
        
        InnerComposer (Op op)
        {
            this.op = op;
        }
        
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new LeftRightExpression(first, second, op);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.LONG);
            
            return argumentTypes.get(0); // this might or might not be the correct precision
        }
        
    };
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private final Op op;
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals, Op op)
        {
            super(evals);
            this.op = op;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource strSource = left();
            if (strSource.isNull()) return NullValueSource.only();
           
            ValueSource lenSource = right();
            if (lenSource.isNull()) return NullValueSource.only();
            
            String str = strSource.getString();
            int len = (int)lenSource.getLong();
            len = len < 0
                    ? 0
                    : len > str.length() ? str.length() : len;

            valueHolder().putString(op.getSubstring(str, len));
            return valueHolder();
        }
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(op);
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations(), op);
    }
    
    protected LeftRightExpression (Expression str, Expression len, Op op)
    {
        super(AkType.VARCHAR, str, len);
        this.op = op;
    }
    
    private final Op op;
}
