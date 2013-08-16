/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.*;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.server.types.extract.LongExtractor;
import com.foundationdb.server.types.extract.ObjectExtractor;
import com.foundationdb.sql.StandardException;
import java.util.List;

public class SubStringExpression extends AbstractCompositeExpression
{
    @Scalar("substring")
    public final static ExpressionComposer COMPOSER = new ExpressionComposer ()
    {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size != 3 && size != 2)
                throw new WrongExpressionArityException(3, size);
            argumentTypes.setType(0, AkType.VARCHAR);
            for (int i = 1; i < size; ++i)
                argumentTypes.setType(i, AkType.LONG);
            return  argumentTypes.get(0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new SubStringExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    };
    
    @Scalar("substr")
    public final static ExpressionComposer COMPOSER_ALIAS = COMPOSER;

    @Override
    public String name()
    {
        return "SUBSTRING";
    }
    
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
            if (st.equals(""))
            {
                valueHolder().putString("");
                return valueHolder();
            }            
                       
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
            
            if (from == 0)
            {
                valueHolder().putString("");
                return valueHolder();
            }
                       
            // if from is negative or zero, start from the end, and adjust
                // index by 1 since index in sql starts at 1 NOT 0
            from += (from < 0?  st.length()  : -1);
          
            // if from is still neg, return empty string
            if (from < 0)
            {
                valueHolder().putString("");
                return valueHolder();
            } 
            
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
            if (to < from || from >= st.length())
            {
                valueHolder().putString("");
                return valueHolder();
            }            
            
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
    public boolean nullIsContaminating()
    {
        return true;
    }
}
