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

public class ReplaceExpression extends AbstractTernaryExpression
{
    @Scalar("replace")
    public static final ExpressionComposer COMPOSER = new TernaryComposer()
    {
        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ReplaceExpression(arguments);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            int length = 0;
            for (int n  = 0; n < argumentTypes.size(); ++n)
            {
                argumentTypes.setType(n, AkType.VARCHAR);
                length += argumentTypes.get(n).getPrecision();
            }
            return ExpressionTypes.varchar(length);
        }
    };

    @Override
    public String name()
    {
        return "REPLACE";
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
            String sts[] = new String[3];
            for (int n = 0; n < sources.length; ++n)
                if (sources[n].isNull()) return NullValueSource.only();
                else sts[n] = sources[n].getString();

            valueHolder().putRaw(AkType.VARCHAR,
                    sts[1].equals("") ? sts[0] : sts[0].replace(sts[1], sts[2]));
            return valueHolder();
                    
        }
    }

    public ReplaceExpression (List<? extends Expression> arguments)
    {
        super(AkType.VARCHAR, arguments);
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("REPLACE");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
