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
import com.akiban.server.expression.ExpressionType;

import java.util.List;

abstract class UnaryComposer implements ExpressionComposer {

    protected abstract Expression compose(Expression argument);
    
    // Most expressions don't need to access the ExpressionType
    // But those that do should override this method.
    protected Expression compose (Expression argument, 
                                  ExpressionType argType, 
                                  ExpressionType returnType)
    {
        throw new UnsupportedOperationException("not supported");
    }
    
    // For most expressions, NULL is contaminating
    // Any expressions that treat NULL specially should override this
    @Override
    public NullTreating getNullTreating()
    {
        return NullTreating.RETURN_NULL;
    }
        
    @Override
    public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
    {
        if (arguments.size() != 1)
            throw new WrongExpressionArityException(1, arguments.size());
        if (arguments.size() + 1 != typesList.size())
            throw new IllegalArgumentException("invalid argc");
        return compose(arguments.get(0), typesList.get(0), typesList.get(1));
    }
    
    @Override
    public Expression compose(List<? extends Expression> arguments) {
        if (arguments.size() != 1)
            throw new WrongExpressionArityException(1, arguments.size());
        return compose(arguments.get(0));
    }
}
