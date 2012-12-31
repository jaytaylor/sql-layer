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
import java.util.Arrays;
import java.util.List;

public class LogExpression extends AbstractCompositeExpression
{
    private static enum Base
    {
        LOG2(2),
        LOG10(10),
        LN(Math.E);

        protected final Expression base;
        private Base (double base)
        {
            this.base = new LiteralExpression(AkType.DOUBLE, base);
        }
    }
    
    @Scalar("log2")
    public static final ExpressionComposer LOG2 = new InnerComposer (Base.LOG2);
    
    @Scalar("log10")
    public static final ExpressionComposer LOG10 = new InnerComposer(Base.LOG10);
    
    @Scalar("ln")
    public static final ExpressionComposer LN = new InnerComposer(Base.LN);
    
    @Scalar("log")
    public static final ExpressionComposer LOG = new  ExpressionComposer ()
    {
        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size != 1 && size != 2)
                throw new WrongExpressionArityException(2, size);
            
            for (int n = 0; n < size; ++n)
                argumentTypes.setType(n, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
        
        @Override
        public String toString ()
        {
            return "LOG";
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            int size = arguments.size();
            if (size != 1 && size != 2)
                throw new WrongExpressionArityException(2, size);
 
            return new LogExpression(arguments, "LOG");
        }
    };
    
    private static class InnerComposer extends UnaryComposer
    {
        private final Base base;
        public InnerComposer (Base base)
        {
            this.base = base;
        }
        
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new LogExpression(Arrays.asList(base.base, argument), base.name());
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1) 
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
        
        @Override
        public String toString ()
        {
            return base.name();
        }
    }

    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation
    {   
        public CompositeEvaluation (List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource arg1 = children().get(0).eval();               
            double num1;
            if (arg1.isNull() || (num1 = arg1.getDouble()) <= 0) 
                return NullValueSource.only();
    
            if (children().size() == 2)
            {            
                double num2; 
                ValueSource arg2 = children().get(1).eval();
                if (arg2.isNull() || num1 == 1 || Double.isInfinite(num1) ||
                        Double.isNaN(num1) || (num2 = arg2.getDouble()) <= 0)
                    return NullValueSource.only();
                
                valueHolder().putDouble(Math.log(num2) / Math.log(num1));
            }
            else
                valueHolder().putDouble(Math.log(num1));
            
            return valueHolder();
        }         
    }
    
    private final String name;

    protected LogExpression (List<? extends Expression> children, String name)
    {
        super(AkType.DOUBLE, children);
        this.name = name;
    }
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name);
    }

    @Override
    public String name ()
    {
        return name;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new CompositeEvaluation(childrenEvaluations());
    }
}
