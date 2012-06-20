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
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.InvalidOperationException;
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
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import java.math.BigDecimal;
import java.math.BigInteger;
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
            // since that's the best we could do for now
            return argumentTypes.get(0);
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

    @Override
    public String name() {
        return "ROUND";
    }
    
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
            
            try
            {
                switch(type)
                {
                    case INT:
                    case LONG:
                    case U_INT:
                        long val = Extractors.getLongExtractor(type).getLong(left);
                        valueHolder().putRaw(type,
                                scale < 0
                                    ? (long)(Math.round(factor * val) / factor)
                                    : val);
                        break;
                    case VARCHAR:   type = AkType.DOUBLE; // fall thru
                    case DOUBLE:
                    case U_DOUBLE:
                    case FLOAT:
                    case U_FLOAT:
                        double rounded = Math.round(factor * Extractors.getDoubleExtractor().getDouble(left)) / factor;
                        switch (type)
                        {
                            case FLOAT:
                            case U_FLOAT : valueHolder().putRaw(type, (float)rounded); break;
                            default:       valueHolder().putRaw(type, rounded);
                        }
                        break;
                    case DECIMAL:
                        BigDecimal num = left.getDecimal();
                        int precision = num.precision() - num.scale() + scale;
                        if (precision <= 0)
                            valueHolder().putDecimal(BigDecimal.ZERO);
                        else
                        {
                            if (scale >= 0) // regular rouding (digits the right of the decimal point)
                                valueHolder().putDecimal(
                                        num.round(new MathContext(precision, RoundingMode.HALF_UP)));
                            else // round  digits to the left of the decimimal point
                            {
                                BigDecimal decFactor = BigDecimal.valueOf(factor);
                                valueHolder().putDecimal(num
                                        .multiply(decFactor, 
                                                  new MathContext(precision,RoundingMode.HALF_UP))
                                        .divide(decFactor, 0, RoundingMode.FLOOR));

                            }
                        }
                        break;
                    case U_BIGINT:
                        if (scale >= 0)
                            valueHolder().putUBigInt(left.getUBigInt());
                        else
                        {
                            BigDecimal decVal = new BigDecimal(left.getUBigInt());
                            int pre = decVal.precision() - decVal.scale() + scale;
                            if (pre <= 0)
                                valueHolder().putUBigInt(BigInteger.ZERO);
                            else
                            {
                                BigDecimal decFactor = BigDecimal.valueOf(factor);
                                valueHolder().putUBigInt(decVal
                                              .multiply(decFactor, 
                                                        new MathContext(pre, RoundingMode.HALF_UP))
                                              .divide(decFactor, 0, RoundingMode.FLOOR).toBigInteger());
                            }
                        }
                        break;

                    default:
                        throw new InvalidArgumentTypeException("Type " + type + "is not supported in ROUND");
                }
                return valueHolder();
            }
            catch (InvalidOperationException e)
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(e);
                return NullValueSource.only();
            }
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
