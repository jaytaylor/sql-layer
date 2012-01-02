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

import com.akiban.server.error.WrongExpressionArityException;
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
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ArgList;
import java.util.List;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.ValueHolder;

public class SubStringExpression extends AbstractCompositeExpression
{
    @Scalar("substring")
    public final static ExpressionComposer COMPOSER = new ExpressionComposer ()
    {

        @Override
        public Expression compose(List<? extends Expression> arguments) {
            return new SubStringExpression(arguments);
        }

        @Override
        public ExpressionType composeType(ArgList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size != 3 && size != 2)
                throw new WrongExpressionArityException(3, size);
            argumentTypes.setArgType(0, AkType.VARCHAR);
            for (int i = 1; i < size; ++i)
                argumentTypes.setArgType(i, AkType.LONG);
            return argumentTypes.get(0);
        }
    };
    
    @Scalar("substr")
    public final static ExpressionComposer COMPOSER_ALIAS = COMPOSER;
    
    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private InnerEvaluation (List<? extends ExpressionEvaluation> children)
        {
            super(children);
        }

        @Override
        public ValueSource eval() 
        {     
            //String operand
            ValueSource stringSource = this.children().get(0).eval();
            if (stringSource.isNull())
                return NullValueSource.only();
            
            ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
            String st = sExtractor.getObject(stringSource);            
            if (st.equals("")) return new ValueHolder(AkType.VARCHAR, "");            
            
            // FROM operand
            int from = 0;
          
            if (this.children().size() >= 2)    
            {
                ValueSource fromSource = this.children().get(1).eval();
                if (fromSource.isNull())
                    return NullValueSource.only();
                LongExtractor iExtractor = Extractors.getLongExtractor(AkType.INT);
                from = (int)iExtractor.getLong(fromSource); 
                
            }
            
            from = (from == 0 ? -1 : from);
            
            // if from is negative or zero, start from the end, and adjust
                // index by 1 since index in sql starts at 1 NOT 0
            from += (from < 0?  st.length()  : -1);
          
            // if from is still neg, set from = 0
            from = from < 0 ? 0 : from;
            
            // TO operand
            int to = st.length() -1;
            
            if (this.children().size() == 3)        
            {
                ValueSource lengthSource = this.children().get(2).eval();
                 if (lengthSource.isNull())
                    return NullValueSource.only();
                LongExtractor iExtractor = Extractors.getLongExtractor(AkType.INT);
                to =  from + (int)iExtractor.getLong(lengthSource) -1 ;
            }
            
            // if to <= fr => return empty
            if (to < from || from >= st.length() ) return new ValueHolder(AkType.VARCHAR, "");            
            to = (to > st.length() -1 ? st.length() -1 : to);

            valueHolder().putString(st.substring(from,to +1 ));
            return valueHolder();
        }  
    }
    
    SubStringExpression (List <? extends Expression> children)
    {
        super(AkType.VARCHAR, children);
        if (children.size() > 3 || children.size() < 2)
            throw new WrongExpressionArityException(3, children.size());        
    }

    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append("SUBSTRING");
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(this.childrenEvaluations());
    }

    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }
}
