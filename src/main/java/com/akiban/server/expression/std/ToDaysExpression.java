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
import com.akiban.server.error.InvalidParameterValueException;
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
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;


public class ToDaysExpression  extends AbstractUnaryExpression
{   
    @Scalar("to_days")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new ToDaysExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.DATE);
            
            return ExpressionTypes.LONG;
        }
    };

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long BEGINNING = Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(33, DateTimeZone.UTC);
        private  static final long FACTOR = 3600L * 1000 * 24;
        
        public InnerEvaluation (ExpressionEvaluation eval)
        {
            super(eval);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource date = operand();
            if (date.isNull())
                return NullValueSource.only();
            
            try
            {
                valueHolder().putLong((Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(date.getDate(), DateTimeZone.UTC)
                                        - BEGINNING) / FACTOR);
                return valueHolder();
            }
            catch ( IllegalFieldValueException e) // zero dates
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(new InvalidParameterValueException(e.getMessage()));
                return NullValueSource.only();
            }
        }
        
    }
    
    ToDaysExpression (Expression arg)
    {
        super(AkType.LONG, arg);
    }
    
    @Override
    public String name()
    {
        return "TO_DAYS";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
