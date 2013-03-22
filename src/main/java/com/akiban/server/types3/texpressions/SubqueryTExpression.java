
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.util.ArgumentValidation;

abstract class SubqueryTExpression implements TPreparedExpression {
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(resultType());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes states = new Attributes();
        states.put(Label.OPERAND, subquery.getExplainer(context)); 
        states.put(Label.OUTER_TYPE, outerRowType == null
                                        ? PrimitiveExplainer.getInstance("<EMPTY>")  
                                        :outerRowType.getExplainer(context));
        states.put(Label.INNER_TYPE, innerRowType == null
                                        ? PrimitiveExplainer.getInstance("<EMPTY>") 
                                        : innerRowType.getExplainer(context));
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

    SubqueryTExpression(Operator subquery, RowType outerRowType,
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
