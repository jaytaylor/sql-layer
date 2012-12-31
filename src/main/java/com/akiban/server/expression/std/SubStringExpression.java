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
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.sql.StandardException;
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
