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
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class FromUnixExpression extends AbstractCompositeExpression
{
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("FROM_UNIXTIME()");
    }
    
    @Scalar("from_unixtime")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer ()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int s = argumentTypes.size();
            switch (s)
            {
                case 2: argumentTypes.setType(1, AkType.VARCHAR);
                        argumentTypes.setType(0, AkType.LONG);
                        return ExpressionTypes.varchar(argumentTypes.get(0).getPrecision() * 5);
                case 1: argumentTypes.setType(0, AkType.LONG);
                        return ExpressionTypes.DATETIME;
                default: throw new WrongExpressionArityException(1, s);
            }
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new FromUnixExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    };

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public String name()
    {
        return "FROM_UNIXTIME";
    }

    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation (List< ? extends ExpressionEvaluation> ev)
        {
            super(ev);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource dateS = children().get(0).eval();
            if (dateS.isNull()) return NullValueSource.only();
            long unix = dateS.getLong() * 1000;

            switch(children().size())
            {
                case 1:     return new ValueHolder(AkType.DATETIME, new DateTime(unix, DateTimeZone.getDefault()));

                default:    ValueSource str = children().get(1).eval();
                            if (str.isNull())
                                return NullValueSource.only();
                            else
                                return new ValueHolder(AkType.VARCHAR, DateTimeField.getFormatted(
                                        new MutableDateTime(unix, DateTimeZone.getDefault()),
                                        str.getString()));
            }

        }
    }

    /**
     * Takes an UNIX_TIME, which is the number of SECONDS from epoch and optionally
     * a format str
     *
     * @param ex
     * @return  DATETIME in <b>current</b> timezone if no format string is passed
     *          VARCHAR representing the datetime formated accordingly if format string is passed
     */
    public FromUnixExpression (List<? extends Expression> ex)
    {
        super(checkArg(ex), ex);
    }

    protected static AkType checkArg (List<? extends Expression> ex)
    {
        if (ex.size() != 2 && ex.size() != 1) throw new WrongExpressionArityException(1, ex.size());
        return ex.size() == 2 ? AkType.VARCHAR : AkType.DATETIME;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
