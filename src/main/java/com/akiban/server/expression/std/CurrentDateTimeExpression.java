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
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.joda.time.DateTime;

public class CurrentDateTimeExpression extends AbstractNoArgExpression
{
    /**
     * return current_date() expression 
     */
    @Scalar({"current_date", "curdate"})
    public static final ExpressionComposer CURRENT_DATE_COMPOSER 
            = new DateTimeComposer(AkType.DATE);
    
    /**
     * return current_time() expression 
     */
    @Scalar({"current_time", "curtime"})
    public static final ExpressionComposer CURRENT_TIME_COMPOSER 
            = new DateTimeComposer(AkType.TIME);
    
    /**
     * return current_timestamp() expression in String
     * current_timestamp, now, localtime and localtimestamp all mean the same thimg
     */
    @Scalar({"current_timestamp", "now", "localtime", "localtimestamp"})
    public static final ExpressionComposer CURRENT_TIMESTAMP_COMPOSER 
            = new DateTimeComposer(AkType.DATETIME);


    private final AkType currentType;

    public CurrentDateTimeExpression(AkType currentType)
    {
        super(checkType(currentType));
        this.currentType = currentType;
    }

    private static final class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        private AkType currentType;
        private QueryContext context;

        public InnerEvaluation(AkType currentType)
        {
            this.currentType = currentType;
        }

        @Override
        public void of(QueryContext context) {
            this.context = context;
        }

        @Override
        public ValueSource eval()
        {
            valueHolder().putRaw(currentType, new DateTime(context.getCurrentDate()));
            return valueHolder();
        }
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    private static final class DateTimeComposer extends NoArgComposer {
        private final AkType currentType;

        public DateTimeComposer(AkType currentType)
        {
            this.currentType = currentType;
        }
        
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(currentType);
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.newType(currentType, 0, 0);
        }        
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(currentType);
    }

    @Override
    public String name ()
    {
        return "CURRENT_" + currentType;
    }
    
    private static AkType checkType (AkType input)
    {
        if (input == AkType.DATE  || input == AkType.TIME || input == AkType.DATETIME) return input;
        else throw new UnsupportedOperationException("CURRENT_" + input + " is not supported");
    }
}
