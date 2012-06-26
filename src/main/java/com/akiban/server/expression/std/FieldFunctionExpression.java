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
import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueSources;
import com.akiban.sql.StandardException;
import java.util.Iterator;
import java.util.List;

public class FieldFunctionExpression extends AbstractCompositeExpression
{
    @Scalar("field")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() < 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            // don't really care about the types (for now)
            return new FieldFunctionExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
        
    };

    @Override
    public String name() {
        return "FIELD_FUNCTION";
    }
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> args)
        {
            super(args);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource first = children().get(0).eval();
            long ret = 0;

            if (!first.isNull())
            {
            
                int n = 0;
                boolean homogeneous = true;
                Iterator<? extends ExpressionEvaluation> iter = children().iterator();
                iter.next();
                while (iter.hasNext())
                    if (!(homogeneous = first.getConversionType() == iter.next().eval().getConversionType()))
                        break;
                
                iter = children().iterator();
                iter.next();
                while (iter.hasNext())
                    try
                    {
                        ValueSource source = iter.next().eval();
                        ++n;
                        if (ValueSources.equals(source, first, !homogeneous))
                        {
                            ret = n;
                            break;
                        } 
                    }
                    catch (InvalidOperationException e)
                    {
                        QueryContext qc = queryContext();
                        if (qc != null)
                            qc.warnClient(e);
                    }
                    catch (NumberFormatException e) // when trying to compare 2 VARCHAR as double
                    {
                        QueryContext qc = queryContext();
                        if (qc != null)
                            qc.warnClient(new InvalidCharToNumException(e.getMessage()));
                    }
                
            }
            
            valueHolder().putLong(ret);
            return valueHolder();
        }
        
    }

    @Override
    public boolean nullIsContaminating()
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
    
    
    FieldFunctionExpression(List<? extends Expression> args)
    {
        super(AkType.LONG, checkArgs(args));
    }
    
    private static List<? extends Expression> checkArgs (List<? extends Expression> args)
    {
        if (args.size() < 2)
            throw new WrongExpressionArityException(2, args.size());
        return args;
    }
}
