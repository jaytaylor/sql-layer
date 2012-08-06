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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;
import java.util.Map;

public abstract class AbstractUnaryExpression implements Expression {

    // Expression interface
    
    // for most expressions this returns TRUE
    // Those that treat NULL specially must override the method
    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        return new ExpressionExplainer(Type.FUNCTION, name(), extraInfo, operand);
    }

    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    public boolean isConstant() {
        return operand.isConstant();
    }

    @Override
    public boolean needsBindings() {
        return operand.needsBindings();
    }

    @Override
    public boolean needsRow() {
        return operand.needsRow();
    }

    @Override
    public AkType valueType() {
        return type;
    }


    // for use by subclasses

    protected final Expression operand() {
        return operand;
    }

    protected final ExpressionEvaluation operandEvaluation() {
        return operand().evaluation();
    }

    protected AbstractUnaryExpression(AkType type, Expression operand) {
        this.type = type;
        this.operand = operand;
    }

    // object interface

    @Override
    public String toString() {
        return name() + "(" + operand + ")";
    }

    // object state

    private final Expression operand;
    private final AkType type;
}
