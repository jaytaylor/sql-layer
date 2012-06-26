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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;

public class IsExpression extends AbstractUnaryExpression
{    
   @Scalar ("istrue")
   public static final ExpressionComposer IS_TRUE = new InnerComposer(TriVal.TRUE);

   @Scalar ("isfalse")
   public static final ExpressionComposer IS_FALSE = new InnerComposer(TriVal.FALSE);

   @Scalar ("isunknown")
   public static final ExpressionComposer IS_UNKNOWN= new InnerComposer(TriVal.UNKNOWN);


   protected static enum TriVal
   {
        TRUE, FALSE, UNKNOWN
   }

   private static class InnerComposer  extends UnaryComposer
   {
       protected final TriVal triVal;
       
       protected InnerComposer (TriVal triVal)
       {
           this.triVal = triVal;
       }

        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new IsExpression(argument, triVal);
        }

       @Override
       public String toString()
       {
           return "IS " + triVal;
       }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.BOOL);
            return ExpressionTypes.BOOL;
        }
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final TriVal triVal;

        public InnerEvaluation (ExpressionEvaluation operandEval, TriVal triVal)
        {
            super(operandEval);
            this.triVal = triVal;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();


            if (source.isNull())            
                return BoolValueSource.of(triVal == TriVal.UNKNOWN);            
            else
                switch (triVal)
                {
                    case TRUE:  return BoolValueSource.of(source.getBool());
                    case FALSE: return BoolValueSource.of(!source.getBool()); 
                    default:    return BoolValueSource.of(false); 
                }
        }
    }

    private final TriVal triVal;
    
    protected IsExpression (Expression arg, TriVal triVal)
    {
        super(AkType.BOOL, arg);
        this.triVal = triVal;
    }

    @Override
    public String name()
    {
        return "IS " + triVal;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), triVal);
    }
}
