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
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import java.util.EnumMap;
import java.util.List;

public class CoalesceExpression extends AbstractCompositeExpression {
    
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Scalar("coalesce")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException 
        {
            // args types don't really need adjusting
            AkType top = getTopType(argumentTypes);
            
            int maxScale = 0, maxPre = 0;
            int scale = 0, pre =0;
            for (int n = 0; n < argumentTypes.size(); ++n)
            {
                scale = argumentTypes.get(n).getScale();
                pre = argumentTypes.get(n).getPrecision();
                
                maxScale = maxScale > scale ? maxScale : scale;
                maxPre = maxPre > pre ? maxPre : pre;
            }
            
            return ExpressionTypes.newType(top, maxPre, maxScale);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new CoalesceExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
    };

    @Override
    public String name () {
        return "COALESCE";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(valueType(), childrenEvaluations());
    }

    @Override
    public boolean nullIsContaminating() {
        return false;
    }

    public CoalesceExpression(List<? extends Expression> children) {
        super(getTopType(children), children);
    }
    
    /**
     * 
     * @param args
     * @return topType (to which all the children can be converted to)
     * 
     * E.g., 
     * (INT, FLOAT, DOUBLE) ===> DOUBLE
     * (DOUBLE, VARCHAR, DATE) ====> VARCHAR
     * (DATE, DATETIME, BOOL) ====> VARCHAR
     * 
     * Basically, the idea is that if the children's types are not mutually convertible
     * to one another, then the top is VARCHAR, because all types can be converted to VARCHAR
     *  
     * Otherwise (also implying that all the children are of numeric types),
     * then type-promotion rules apply. 
     * 
     */
     protected static AkType getTopType(List<?> args)
     {
        if (args.isEmpty()) throw new WrongExpressionArityException(2, 0);
        int n = 0;
        AkType top;

        do
        {
            top = getAk(args.get(n++));
        }while (top == AkType.NULL && n < args.size());

        for (; n < args.size(); ++n)
        {
            AkType iter = getAk(args.get(n));
            if (iter != top && iter != AkType.NULL)
            {
                if (NUMERICS.containsKey(top) && NUMERICS.containsKey(iter))
                    top = NUMERICS.get(top) <= NUMERICS.get(iter) ? top : iter;
                else
                    return AkType.VARCHAR;
            }
        }
        return top;
    }

    private static AkType getAk (Object arg)
    {
        if (arg instanceof AkType)
            return (AkType)arg;
        else if (arg instanceof ExpressionType)
            return ((ExpressionType)arg).getType();
        else if (arg instanceof Expression)
            return ((Expression)arg).valueType();
        else
            throw new RuntimeException ("Unexpected args");

    }
    
    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation {

        @Override
        public ValueSource eval() {
            for (ExpressionEvaluation childEvaluation : children()) {
                ValueSource childSource = childEvaluation.eval();
                if (!childSource.isNull()) {
                    return  Converters.convert(childSource, holder());
                }
            }
            return NullValueSource.only();
        }

        private InnerEvaluation(AkType type, List<? extends ExpressionEvaluation> children) {
            super(children);
            this.type = type;
        }

        private ValueHolder holder() {
            valueHolder().expectType(type);
            return valueHolder();
        }

        private final AkType type;        
    }
    
    private static final EnumMap<AkType, Integer> NUMERICS = new EnumMap<AkType, Integer>(AkType.class);
    static
    {
        // putting all the numeric types in a prioritised order (highest first) 
        NUMERICS.put(AkType.DECIMAL, 0);
        NUMERICS.put(AkType.DOUBLE, 2);
        NUMERICS.put(AkType.U_DOUBLE, 1);
        NUMERICS.put(AkType.U_FLOAT, 2);
        NUMERICS.put(AkType.FLOAT, 3);
        NUMERICS.put(AkType.U_BIGINT, 4);
        NUMERICS.put(AkType.LONG, 5);
        NUMERICS.put(AkType.INT, 6);
        NUMERICS.put(AkType.U_INT, 7);
    }
}
