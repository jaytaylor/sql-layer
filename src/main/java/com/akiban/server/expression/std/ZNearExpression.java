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
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

public class ZNearExpression extends AbstractCompositeExpression {

    @Scalar("znear")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            int size = argumentTypes.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            
            for (int i = 0; i < size; ++i) {
                argumentTypes.setType(i, AkType.DECIMAL);
            }
            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList) {
            int size = arguments.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            return new ZNearExpression(arguments);     
        }

        @Override
        public ExpressionComposer.NullTreating getNullTreating() {
            return ExpressionComposer.NullTreating.RETURN_NULL;        
        }
    };
    
    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation {        
        public CompositeEvaluation (List<? extends ExpressionEvaluation> childrenEvals) {
            super(childrenEvals);
        }
        
        @Override
        public ValueSource eval() {
            // No operation necessary
            throw new UnsupportedOperationException();
        }
    }
    
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new CompositeEvaluation(childrenEvaluations());
    }

    @Override
    public String name() {
        return "ZNEAR";
    }
    
    private static List<? extends Expression> checkArity(List<? extends Expression> children) {
        int size = children.size();
        if (size != 4) throw new WrongExpressionArityException(4, size);
        return children;
    }
    
    protected ZNearExpression(List<? extends Expression> children) {
        super(AkType.LONG, checkArity(children));
    }

}
