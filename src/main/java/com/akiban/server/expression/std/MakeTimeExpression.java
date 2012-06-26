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
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.Arrays;
import java.util.List;

public class MakeTimeExpression extends AbstractTernaryExpression
{
    @Scalar("maketime")
    public static final ExpressionComposer COMPOSER = new TernaryComposer()
    {
        
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            for (int n = 0; n < argumentTypes.size(); n++) 
            {
                argumentTypes.setType(n, AkType.LONG);
            }
            return ExpressionTypes.TIME;
            
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new MakeTimeExpression(arguments);
        }

        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new MakeTimeExpression(arguments);
        }
        
    };

    @Override
    public String name() {
        return "MAKETIME";
    }
    
    private static final class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource sources[] = getAll();
            for (int n = 0; n < sources.length; n++) 
            {
                if (sources[n].isNull()) return NullValueSource.only();
            }
            // Time input format HHMMSS
            long hours = sources[0].getLong();
            long minutes = sources[1].getLong();
            long seconds = sources[2].getLong();
            
            // Check for valid input
            if (minutes >= 60 || minutes < 0) return NullValueSource.only();
            if (seconds >= 60 || seconds < 0) return NullValueSource.only();
            
            long time = hours < 0 ? -1 : 1;
            hours *= time;
            time *= seconds + minutes * 100 + hours * 10000;
            valueHolder().putTime(time);
            return valueHolder();
        }
                
    }
    
    public MakeTimeExpression (List<? extends Expression> args)
    {
        super(AkType.TIME, args);
    }

    // for testing
    MakeTimeExpression (Expression arg1, Expression arg2, Expression arg3)
    {
        this(Arrays.asList(arg1, arg2, arg3));
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
