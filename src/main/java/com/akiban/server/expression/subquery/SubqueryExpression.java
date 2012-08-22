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

package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.expression.Expression;
import com.akiban.util.ArgumentValidation;
import java.util.Map;

public abstract class SubqueryExpression implements Expression {

    @Override
    public boolean nullIsContaminating() {
        return true;
    }
    
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    @Override
    public boolean needsRow() {
        return (outerRowType != null);
    }
    
    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo) {
        
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance(name()));
        states.put(Label.OPERAND, subquery.getExplainer(extraInfo)); 
        states.put(Label.OUTER_TYPE, PrimitiveExplainer.getInstance(outerRowType.toString()));
        states.put(Label.INNER_TYPE, PrimitiveExplainer.getInstance(innerRowType.toString()));
        states.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        
        return new CompoundExplainer(Type.SUBQUERY, states);
    }

    // for use by subclasses

    protected final Operator subquery() {
        return subquery;
    }
    protected final RowType outerRowType() {
        return outerRowType;
    }
    protected final RowType innerRowType() {
        return innerRowType;
    }
    protected final int bindingPosition() {
        return bindingPosition;
    }

    protected SubqueryExpression(Operator subquery, RowType outerRowType, 
                                 RowType innerRowType, int bindingPosition) {
        ArgumentValidation.notNull("subquery", subquery);
        ArgumentValidation.isGTE("binding position", bindingPosition, 0);
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;

}
