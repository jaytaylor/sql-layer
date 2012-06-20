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
import java.io.UnsupportedEncodingException;
import org.slf4j.LoggerFactory;

public class BitLengthExpression extends AbstractUnaryExpression
{
    @Scalar("bit_length")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            String charset = "UTF-8";
            if ((argType != null) &&
                (argType.getCharacterAttributes() != null) &&
                (argType.getCharacterAttributes().getCharacterSet() != null))
                charset = argType.getCharacterAttributes().getCharacterSet();
            return new BitLengthExpression(argument, charset);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
           if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
           argumentTypes.setType(0, AkType.VARCHAR);
           return ExpressionTypes.LONG;
        }
    };

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final String charset;
        public InnerEvaluation (ExpressionEvaluation operand, String charset)
        {
            super(operand);
            this.charset = charset;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
             else
                 try 
                 {
                     return new ValueHolder(AkType.LONG, source.getString().getBytes(charset).length * 8);
                 }
                 catch (UnsupportedEncodingException ex)
                 {
                     LoggerFactory.getLogger(BitLengthExpression.class).error("Un-recognised charset", ex);
                     return NullValueSource.only();
                 }
        }
    }

    public BitLengthExpression (Expression arg)
    {
        super(AkType.LONG, arg);
        CHARSET = null;
    }

    public BitLengthExpression (Expression arg, String charset)
    {
        super(AkType.LONG, arg);
        this.CHARSET = charset;
    }
    
    @Override
    public String name()
    {
        return "BIT_LENGTH";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), CHARSET);
    }

    private final String CHARSET;
}
