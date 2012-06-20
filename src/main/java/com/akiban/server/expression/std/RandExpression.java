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
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;
import java.util.Random;

public class RandExpression extends AbstractCompositeExpression
{
    @Scalar({"random", "rand"})
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            switch(argumentTypes.size())
            {
                case 1:     argumentTypes.setType(0, AkType.LONG); // fall thru
                case 0:     break;
                default:    throw new WrongExpressionArityException(1, argumentTypes.size());
            }

            return ExpressionTypes.DOUBLE;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new RandExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
        
    };

    @Override
    public String name() {
        return "RAND";
    }
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private  Random random;
        
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
            random = null; 
        }
        
        @Override
        public ValueSource eval()
        {
            if (random == null)
                switch(children().size())
                {
                    case 0:     
                        random = new Random(); 
                        break;

                    case 1:     
                        ValueSource source = children().get(0).eval();
                        if (source.isNull())
                            random = new Random(0L);
                         else
                            random = new Random(source.getLong());
                         break;

                    default:    
                        throw new WrongExpressionArityException(1, children().size());
                }
            
            valueHolder().putDouble(random.nextDouble());
            return valueHolder();
        }
        
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return false;
    }

    @Override
    public boolean isConstant()
    {
        return false;
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
    
    protected RandExpression (List<? extends Expression> exps)
    {
        super(checkArgs(exps), exps);
    }
    
    private static AkType checkArgs(List<? extends Expression> args)
    {
        if (args.size() != 0 && args.size() != 1)
            throw new WrongExpressionArityException(1, args.size());
        return AkType.DOUBLE;
            
    }
}
