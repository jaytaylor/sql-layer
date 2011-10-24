/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.expression.std;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.DoubleExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.util.List;

public class TrigExpression extends AbstractCompositeExpression
{ 
    public static enum TrigName
    {
        SIN, COS, TAN, COT, ASIN, ACOS, ATAN, ATAN2, COSH
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
    
    private static class InternalComposer implements ExpressionComposer
    {
        private final TrigName name;
        
        public InternalComposer (TrigName name)
        {
            this.name = name;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments) 
        {
            return new TrigExpression(arguments, name);
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
            
            // get second operand, if name == ATAN2
            double dvar2 = 1;
            if (name.equals(TrigName.ATAN2))
            {
                ValueSource secOperand = this.children().get(1).eval();
                if (secOperand.isNull())
                    return NullValueSource.only();
                
                dvar2 = dExtractor.getDouble(secOperand);
            }
            
            double result = 0;       
            switch (name)
            {
                case SIN: result = Math.sin(dvar1); break;              
                case COS: result = Math.cos(dvar1); break;       
                case TAN: result = Math.tan(dvar1);break;      
                case COT: result = Math.cos(dvar1) / Math.sin(dvar1); break;      
                case ASIN: result = Math.asin(dvar1); break;
                case ACOS: result = Math.acos(dvar1); break;
                case ATAN: result = Math.atan(dvar1); break;
                case ATAN2: result = Math.atan2(dvar1, dvar2); break;
                case COSH: result = Math.cosh(dvar1); break;
            }
            
            return new ValueHolder(AkType.DOUBLE, result);
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
        
        if (name.equals(TrigName.ATAN2))
        {   
            if (children.size() != 2)
                throw new AkibanInternalException("Ilegal number of Arguments");
        }
        else
            if (children.size() != 1)
                throw new AkibanInternalException("Ilegal number of Arguments");
     
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
}
