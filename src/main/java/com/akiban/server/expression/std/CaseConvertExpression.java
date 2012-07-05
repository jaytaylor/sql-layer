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
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;

public class CaseConvertExpression extends AbstractUnaryExpression
{
    public static enum ConversionType
    {
        TOUPPER,
        TOLOWER 
    }
    
    private final ConversionType conversionType;
    
    @Scalar ({"lcase", "lower"})
    public static final ExpressionComposer TOLOWER_COMPOSER = new InternalComposer(ConversionType.TOLOWER);
    
    @Scalar ({"ucase", "upper"})
    public static final ExpressionComposer TOUPPER_COMPOSER = new InternalComposer(ConversionType.TOUPPER);
    
    private static final class InternalComposer extends UnaryComposer
    {
        private final ConversionType conversionType;

        public InternalComposer (ConversionType conversionType)
        {
            this.conversionType = conversionType;
        }
        
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) 
        {
            return new CaseConvertExpression(argument, conversionType);
        }         

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.VARCHAR);
            return argumentTypes.get(0);
        }
    }
    
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final ConversionType conversionType;
        
        public InnerEvaluation (ExpressionEvaluation ev, ConversionType conversionType)
        {
            super(ev);
            this.conversionType = conversionType;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            
            ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
            String st = sExtractor.getObject(operandSource);
            valueHolder().putString(conversionType == ConversionType.TOLOWER ? st.toLowerCase() : st.toUpperCase());
            return valueHolder();
        }         
    }
     
    /**
     * 
     * @param operand
     * @param type 
     */
    public CaseConvertExpression (Expression operand, ConversionType type)
    {
        super(AkType.VARCHAR, operand);
        this.conversionType = type;
    }

    @Override
    public String name() 
    {
        return conversionType.name();
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(this.operandEvaluation(), conversionType);
    }
}
