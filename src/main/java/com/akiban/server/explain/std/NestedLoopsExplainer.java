
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;

public class NestedLoopsExplainer extends CompoundExplainer
{
    public NestedLoopsExplainer (String name, Operator innerOp, Operator outerOp, RowType innerType, RowType outerType, ExplainContext context)
    {
        super(Type.NESTED_LOOPS, buildMap(name, innerOp, outerOp, innerType, outerType, context));
    }
    
    private static Attributes buildMap (String name, Operator innerOp, Operator outerOp, RowType innerType, RowType outerType, ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.INPUT_OPERATOR, outerOp.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, innerOp.getExplainer(context));
        if (innerType != null)
            atts.put(Label.INNER_TYPE, innerType.getExplainer(context));
        if (outerType != null)
            atts.put(Label.OUTER_TYPE, outerType.getExplainer(context));
        
        return atts;
    }
}
