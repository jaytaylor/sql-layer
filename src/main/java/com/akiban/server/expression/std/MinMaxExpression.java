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
import com.akiban.server.types.util.ValueSources;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import java.util.Arrays;
import java.util.List;

/** Note: This isn't the <code>MAX</code> aggregate function, but its scalar cousin. */
public class MinMaxExpression extends AbstractBinaryExpression
{

    @Override
    public String name() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public static enum Operation { MIN, MAX }
    
    private final Operation operation;
    
    @Scalar ("_min")
    public static final ExpressionComposer MIN_COMPOSER = new InternalComposer(Operation.MIN);
    
    @Scalar ("_max")
    public static final ExpressionComposer MAX_COMPOSER = new InternalComposer(Operation.MAX);
    
    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        final Operation operation;
        
        public InnerEvaluation (List< ? extends ExpressionEvaluation> children, Operation operation)
        {
            super(children);
            this.operation = operation;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource v1 = left();
            if (v1.isNull()) return NullValueSource.only();
            
            ValueSource v2 = right();
            if (v2.isNull()) return NullValueSource.only();
            
            return (((ValueSources.compare(v1, v2) > 0) == (operation == Operation.MAX)) ? v1 : v2);
        }
    }
    
    private static final class InternalComposer extends BinaryComposer
    {
        private final Operation operation;
        
        public InternalComposer (Operation operation)
        {
            this.operation = operation;
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            AkType topType = CoalesceExpression.getTopType(Arrays.asList(argumentTypes.get(0).getType(), 
                                                                         argumentTypes.get(1).getType()));
            argumentTypes.setType(0, topType);
            argumentTypes.setType(1, topType);
            return ExpressionTypes.newType(topType,
                                           Math.max(argumentTypes.get(0).getPrecision(), argumentTypes.get(1).getPrecision()),
                                           Math.max(argumentTypes.get(0).getScale(), argumentTypes.get(1).getScale()));
        }

        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new MinMaxExpression(first, second, operation);
        }
    }
  
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(operation);
    }
    
    public MinMaxExpression(Expression first, Expression second, Operation operation)
    {
        super(CoalesceExpression.getTopType(Arrays.asList(first.valueType(), second.valueType())), 
              first, second);
        this.operation = operation;
    }
    
    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluations(), operation);
    }    
}
