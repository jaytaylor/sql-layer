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

abstract class BinaryComposer implements ExpressionComposer {

    protected abstract Expression compose(Expression first, Expression second);

    // Most expessions don't need access to types list
    // Those that do need to override this
    protected Expression compose (List<? extends Expression> arguments,
                                ExpressionType first, ExpressionType second, ExpressionType top)
    {
        throw new UnsupportedOperationException("not supported");
    }
    
    @Override
    public Expression compose (List<? extends Expression> arguments, List<ExpressionType> typesList)
    {
        if (arguments.size() != 2)
            throw new WrongExpressionArityException(2, arguments.size());
        if (arguments.size() + 1 != typesList.size())
            throw new IllegalArgumentException("unexpected argc");
        return compose(arguments, typesList.get(0), typesList.get(1), typesList.get(2));
    }
    
    @Override
    public Expression compose(List<? extends Expression> arguments) {
        if (arguments.size() != 2)
            throw new WrongExpressionArityException(2, arguments.size());
        return compose(arguments.get(0), arguments.get(1));
    }
}
