
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.Operator;
import com.akiban.server.explain.*;

public class DUIOperatorExplainer extends CompoundExplainer
{
    public DUIOperatorExplainer (String name, Attributes atts, Operator inputOp, ExplainContext context)
    {
        super(Type.DUI, buildAtts(name, atts, inputOp, context));
    }
    
    private static Attributes buildAtts (String name, Attributes atts, Operator inputOp, ExplainContext context)
    {
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        try {
            atts.put(Label.TABLE_TYPE, inputOp.rowType().getExplainer(context));
        }
        catch (UnsupportedOperationException exception) {
        }
        return atts;
    }
}
