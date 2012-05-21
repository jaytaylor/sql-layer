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
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;

public class RoundExpression extends AbstractCompositeExpression
{

    @Scalar("round")
    public static final ExpressionComposer ROUND = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            switch(argumentTypes.size())
            {
                case 2:  argumentTypes.setType(1, AkType.LONG); // fall thru
                case 1:  break;
                default: throw new WrongExpressionArityException(2, argumentTypes.size());   
            }
            
            // return type has the same type with the same preceiosn/scale as the argument
            return argumentTypes.get(0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new RoundExpression(arguments);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new RoundExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
    };
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource left = children().get(0).eval();
            ValueSource right;
            
            if (left.isNull())
                return NullValueSource.only();
            
            int scale = 0;
            if (children().size() == 2)
            {
                if ((right = children().get(1).eval()).isNull())
                    return NullValueSource.only();
               scale = (int)right.getLong(); 
            }
            
            AkType type = left.getConversionType();
            double factor = Math.pow(10, scale);
            switch(type)
            {
                case INT:
                case LONG:
                case U_INT:
                    valueHolder().putRaw(type,
                            (long)(Math.round(factor * Extractors.getDoubleExtractor().getDouble(left)) / factor));
                    break;
                case DOUBLE:
                case U_DOUBLE:
                    valueHolder().putRaw(type,
                            Math.round(factor * Extractors.getDoubleExtractor().getDouble(left)) / factor);
                    break;
                case FLOAT:
                case U_FLOAT:
                    valueHolder().putRaw(type,
                            (float)(Math.round(factor * Extractors.getDoubleExtractor().getDouble(left)) / factor));
                    break;
                case DECIMAL:
                    BigDecimal num = left.getDecimal();
                    
                    if (scale >= 0)
                        valueHolder().putDecimal(
                                num.round(new MathContext(scale + num.precision() - num.scale(), RoundingMode.HALF_UP)));
                    else
                        valueHolder().putDecimal(
                                num.multiply(BigDecimal.valueOf(factor), new MathContext(num.)))
                    break;
                case U_BIGINT:
                    
//                    valueHolder().putRaw(type,
//                            left.getUBigInt().);
                default:
                    // TODO: return NULL and warnings
                    throw new UnsupportedOperationException("not supported for now");
            }
            
            return valueHolder();
        }
        
    }
    
    RoundExpression (List<? extends Expression> args)
    {
        super(getTopType(args), args);
    }
    
    private static AkType getTopType(List<? extends Expression> args)
    {
        if (args.size() != 1 & args.size() != 2)
            throw new WrongExpressionArityException(2, args.size());
        return args.get(0).valueType();
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("ROUND");
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
