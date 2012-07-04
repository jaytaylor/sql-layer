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
import com.akiban.server.error.OverflowException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.DoubleExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import java.util.List;

public class TrigExpression extends AbstractCompositeExpression
{
    @Override
    public String name()
    {
        return name.name();
    }
    
    public static enum TrigName
    {
        SIN, COS, TAN, COT, ASIN, ACOS, ACOT, ATAN, ATAN2, COSH, SINH, TANH, COTH
    }
    
    private final TrigName name;
    
    @Scalar ("sin")
    public static final ExpressionComposer SIN_COMPOSER = new InternalComposer (TrigName.SIN);
    
    @Scalar ("cos")
    public static final ExpressionComposer COS_COMPOSER = new InternalComposer(TrigName.COS);
    
    @Scalar ("tan")
    public static final ExpressionComposer TAN_COMPOSER = new InternalComposer(TrigName.TAN);
    
    @Scalar ("cot")
    public static final ExpressionComposer COT_COMPOSER = new InternalComposer(TrigName.COT);
    
    @Scalar ("asin")
    public static final ExpressionComposer ASIN_COMPOSER = new InternalComposer(TrigName.ASIN);
    
    @Scalar ("acos")
    public static final ExpressionComposer ACOS_COMPOSER = new InternalComposer(TrigName.ACOS);
    
    @Scalar ("atan")
    public static final ExpressionComposer ATAN_COMPOSER = new InternalComposer(TrigName.ATAN);
    
    @Scalar ("atan2")
    public static final ExpressionComposer ATAN2_COMPOSER = new InternalComposer(TrigName.ATAN2);
    
    @Scalar ("cosh")
    public static final ExpressionComposer COSH_COMPOSER = new InternalComposer(TrigName.COSH);
    
    @Scalar ("sinh")
    public static final ExpressionComposer SINH_COMPOSER = new InternalComposer(TrigName.SINH);
    
    @Scalar ("tanh")
    public static final ExpressionComposer  TANH_COMPOSER = new InternalComposer(TrigName.TANH);
    
    @Scalar ("coth")
    public static final ExpressionComposer COTH_COMPOSER = new InternalComposer(TrigName.COTH);
    
    @Scalar ("acot")
    public static final ExpressionComposer ACOT_COMPOSER = new InternalComposer(TrigName.ACOT);
    
    private static class InternalComposer implements ExpressionComposer
    {
        private final TrigName name;
        
        public InternalComposer (TrigName name)
        {
            this.name = name;
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();

            switch(size)
            {
                case 2:     if (name != TrigName.ATAN && name != TrigName.ATAN2)
                                throw new WrongExpressionArityException(1, size); // fall thru
                case 1:     break;
                default:    throw new WrongExpressionArityException(1, size);
            }
           
            for (int i = 0; i < size; ++i)
                argumentTypes.setType(i, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new TrigExpression(arguments, name);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    }
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private final TrigName name;   

        public InnerEvaluation (List<? extends ExpressionEvaluation> children, TrigName name)
        {
             super(children);  
             this.name = name;
        }

        @Override
        public ValueSource eval() 
        {
            // get first operand
            ValueSource firstOperand = this.children().get(0).eval();
            if (firstOperand.isNull())
                return NullValueSource.only();
            
            DoubleExtractor dExtractor = Extractors.getDoubleExtractor();
            double dvar1 = dExtractor.getDouble(firstOperand);
            
            // get second operand, if any, for ATAN2/ATAN
            double dvar2 = 1;
            if (children().size() == 2)
            {
                ValueSource secOperand = this.children().get(1).eval();
                if (secOperand.isNull())
                    return NullValueSource.only();
                
                dvar2 = dExtractor.getDouble(secOperand);
            }
           
            double result = 0;
            double temp;
            switch (name)
            {
                case SIN:   result = Math.sin(dvar1); break;              
                case COS:   result = Math.cos(dvar1); break; 
                case TAN:   if ( Math.cos(dvar1) == 0)
                                throw new OverflowException ();
                            else result = Math.tan(dvar1);
                            break;
                case COT:   if ((temp = Math.sin(dvar1)) == 0)
                                throw new OverflowException ();
                            else result = Math.cos(dvar1) / temp;
                            break;
                case ACOT:  if ( dvar1 == 0) result = Math.PI /2;
                            else result = Math.atan(1 / dvar1);
                            break;
                case ASIN:  result = Math.asin(dvar1); break;
                case ACOS:  result = Math.acos(dvar1); break;                    
                case ATAN:  
                case ATAN2: result = Math.atan2(dvar1, dvar2); break;                    
                case COSH:  result = Math.cosh(dvar1); break;
                case SINH:  result = Math.sinh(dvar1); break;
                case TANH:  result = Math.tanh(dvar1); break;
                case COTH:  if (dvar1 == 0)
                                throw new OverflowException ();
                            else result = Math.cosh(dvar1) / Math.sinh(dvar1);
                            break;
                default: throw new UnsupportedOperationException("Unknown Operation: " + name.name());
            }

            valueHolder().putDouble(result);
            return valueHolder();
        }   
    }
     
    /**
     * Creates a trigonometry expression (SIN, COS, ...., ATAN, ATAN2, or COSH)
     * TrigExpression is null if its argument is null
     * @param children
     * @param name 
     */
    public TrigExpression (List <? extends Expression> children, TrigName name)
    { 
        super (AkType.DOUBLE, children);
        
        int size = children.size();
        switch(size)
        {
            case 2:     if (name != TrigName.ATAN && name != TrigName.ATAN2)
                            throw new WrongExpressionArityException(1, size); // fall thru
            case 1:     break;
            default:    throw new WrongExpressionArityException(1, size);
        }
        this.name = name;
    }
    
    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append(name.name()).append("_EXPRESSION");
    }
   
    @Override
    public ExpressionEvaluation evaluation() 
    {
       return new InnerEvaluation (this.childrenEvaluations(), name);
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
}
