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
import com.akiban.server.expression.ExpressionComposer.NullTreating;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.Iterator;
import java.util.List;

public class ConcatWSExpression extends AbstractCompositeExpression
{
    @Scalar("concat_ws")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() < 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            
            int len = 0;
            int dLen = argumentTypes.get(0).getPrecision();
            
            for (int n = 1; n < argumentTypes.size(); ++n)
            {
                argumentTypes.setType(n, AkType.VARCHAR);
                len += argumentTypes.get(n).getPrecision() + dLen;
            }
            len -= dLen; // delete the last delilmeter

            return ExpressionTypes.varchar(len);            
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ConcatWSExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            // TODO: could improve this by having the ConstantFolder
            // remove all the NULLs except the first one
            return NullTreating.IGNORE;
        }
    };
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> args)
        {
            super(args);
        }

        @Override
        public ValueSource eval()
        {
            Iterator<? extends ExpressionEvaluation> iter = children().iterator();
            ValueSource delimeterSource = iter.next().eval();
            
            if (delimeterSource.isNull())
                return NullValueSource.only();
            
            String delimeter = delimeterSource.getString();
            StringBuilder bd = new StringBuilder();

            while (iter.hasNext())
            {
                ValueSource arg = iter.next().eval();
                if (!arg.isNull())
                    bd.append(arg.getString()).append(delimeter);
            }
            //remove the last delimeter
            bd.delete(bd.length() - delimeter.length(),
                      bd.length());

            valueHolder().putString(bd.toString());
            return valueHolder();
        }
    }

    protected ConcatWSExpression(List<? extends Expression> args)
    {
        super(AkType.VARCHAR, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("CONCAT_WS");
    }

    @Override
    public boolean nullIsContaminating()
    {
        // NULL is comtaminating only when it's the first arg
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public String name()
    {
        return "CONCAT_WS";
    }
}
