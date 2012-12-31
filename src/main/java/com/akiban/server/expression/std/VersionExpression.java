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

import com.akiban.server.AkServer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public abstract class VersionExpression extends AbstractNoArgExpression
{
    @Scalar("version_full")
    public static final ExpressionComposer FULL_COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return FULL_VERSION;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(FULL_VERSION_SOURCE.getString().length());
        }
    };

    @Scalar("version")
    public static final ExpressionComposer SHORT_COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return SHORT_VERSION;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(SHORT_VERSION_SOURCE.getString().length());
        }
    };
    
    private VersionExpression()
    {
        super(AkType.VARCHAR);
    }

    @Override
    public String name()
    {
        return "version";
    }
    
    // static members

    private static final ValueSource FULL_VERSION_SOURCE = new ValueHolder(AkType.VARCHAR, AkServer.VERSION_STRING);
    private static final ValueSource SHORT_VERSION_SOURCE = new ValueHolder(AkType.VARCHAR, AkServer.SHORT_VERSION_STRING);
    
    private static final Expression FULL_VERSION = new VersionExpression()
    {
        @Override
        public ExpressionEvaluation evaluation()
        {
            return FULL_EVAL;
        }
    };
    
    private static final Expression SHORT_VERSION = new VersionExpression()
    {
        @Override
        public ExpressionEvaluation evaluation()
        {
            return SHORT_EVAL;
        }
    };
    
    private static final ExpressionEvaluation FULL_EVAL = new AbstractNoArgExpressionEvaluation()
    {
        @Override
        public ValueSource eval()
        {
            return FULL_VERSION_SOURCE;
        }
    };
    
    private static final ExpressionEvaluation SHORT_EVAL = new AbstractNoArgExpressionEvaluation()
    {
        @Override
        public ValueSource eval()
        {
            return SHORT_VERSION_SOURCE;
        }   
    };
}
