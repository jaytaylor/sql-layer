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
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import java.util.List;

public class TrimExpression extends AbstractBinaryExpression
{
    public static enum TrimType { LEADING, TRAILING}
    
    private final TrimType trimType;
    
    @Scalar ("ltrim")
    public static final ExpressionComposer LTRIM_COMPOSER = new InternalComposer(TrimType.LEADING);
    
    @Scalar ("rtrim")
    public static final ExpressionComposer RTRIM_COMPOSER = new InternalComposer(TrimType.TRAILING);
    
    @Scalar ("trim")
    public static final ExpressionComposer TRIM_COMPOSER = new InternalComposer(null);
    
    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        final TrimType trimType;
        
        public InnerEvaluation (List< ? extends ExpressionEvaluation> children, TrimType trimType)
        {
            super(children);
            this.trimType = trimType;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource trimSource = left();
            if (trimSource.isNull()) return NullValueSource.only();
            
            ValueSource trimChar = right();
            if (trimChar.isNull()) return NullValueSource.only();
            
            String st = trimSource.getString();
            char ch = trimChar.getString().charAt(0);

            if (trimType != TrimType.TRAILING)
                st = ltrim(st, ch);
            if (trimType != TrimType.LEADING)
                st = rtrim(st, ch);          
            
            valueHolder().putString(st);
            return valueHolder();
        }
        
        private static String ltrim (String st, char ch)
        {
            for (int n = 0; n < st.length(); ++n)
                if (st.charAt(n) != ch)
                    return st.substring(n);
            return "";
        }
        
        private static String rtrim (String st, char ch)
        {
            for (int n = st.length() - 1; n >= 0; --n)
                if(st.charAt(n) != ch)
                    return st.substring(0, n+1);                   
            return "";
        }        
    }
    
    private static final class InternalComposer extends BinaryComposer
    {
        private final TrimType trimType;
        
        public InternalComposer (TrimType trimType)
        {
            this.trimType = trimType;
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            for (int n = 0; n < argumentTypes.size(); ++n)
                argumentTypes.setType(n, AkType.VARCHAR);
            return argumentTypes.get(0);
        }

        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new TrimExpression(first, second, trimType);
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
        sb.append(trimType);
    }
    
    /**
     * type specifies whether to trim trailing or leading
     *      if type = RTRIM => trailing
     *         type = LTRIM => leading
     *         anything else => both trailing and leading
     * @param operand
     * @param type 
     */
    public TrimExpression (Expression first, Expression second,TrimType type)
    {
        super(AkType.VARCHAR, first, second);
        this.trimType = type;
    }
    
    @Override
    public String name() 
    {       
        return "TRIM " + (trimType == null ? "" : trimType.name());
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluations(), trimType);
    }    
}
