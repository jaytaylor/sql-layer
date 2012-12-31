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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import java.io.UnsupportedEncodingException;
import org.slf4j.LoggerFactory;

/**
 * 
 * This implements the (byte) LENGTH function, which returns the number of byte in
 * the string argument. 
 * 
 * This differs from LengthExpression in that if a string st has 5 2-byte characters
 *      - LenthExpression would return 5
 *      - OctetLengthExpression would return 10
 * 
 */
public class OctetLengthExpression extends AbstractUnaryExpression
{
    @Scalar ("getOctetLength")
    public static final ExpressionComposer COMPOSER = new LengthExpression.InternalComposer()
    {       
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) 
        {
            String charset = "UTF-8";
            if ((argType != null) &&
                (argType.getCharacterAttributes() != null) &&
                (argType.getCharacterAttributes().getCharacterSet() != null))
                charset = argType.getCharacterAttributes().getCharacterSet();
            return new OctetLengthExpression(charset, argument);
        }
    };
        
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private String charset;

        public InnerEvaluation (String charset, ExpressionEvaluation ev)
        {
            super(ev);
            this.charset = charset;
        }

        @Override
        public ValueSource eval() 
        {
           ValueSource source = this.operand();
           if (source.isNull()) return NullValueSource.only();
           
           ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
           String st = sExtractor.getObject(source);
           
           try {
               valueHolder().putLong(st.getBytes(charset).length);
           }
           catch (UnsupportedEncodingException ex) {
               LoggerFactory.getLogger(OctetLengthExpression.class).error("Un-recognised charset", ex);
               return NullValueSource.only();
           }
           return valueHolder();
        }        
    }
    
    public OctetLengthExpression (String charset, Expression e)
    {
        super(AkType.LONG, e);
        this.charset = charset;
    }

    @Override
    public String name() 
    {
        return "LENGTH";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(charset, operandEvaluation());
    }    

    private final String charset;
}
