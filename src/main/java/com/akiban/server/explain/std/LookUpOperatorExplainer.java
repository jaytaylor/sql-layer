
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;

public class LookUpOperatorExplainer extends CompoundExplainer
{
    public LookUpOperatorExplainer (String name, Attributes atts, RowType iRowType, boolean keepInput, Operator inputOp, ExplainContext context)
    {
        super(Type.LOOKUP_OPERATOR, buildAtts(name, atts, iRowType, keepInput, inputOp, context));
    }
    
    private static Attributes buildAtts (String name, Attributes atts, RowType iRowType, boolean keepInput, Operator inputOp, ExplainContext context)
    {
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.ROWTYPE, iRowType.getExplainer(context));
        atts.put(Label.INPUT_TYPE, iRowType.getExplainer(context));
        atts.put(Label.INPUT_PRESERVATION, PrimitiveExplainer.getInstance((keepInput ? "KEEP_INPUT" : "DISCARD_INPUT")));
        if (null != inputOp)
            atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        return atts;
    }
}
