
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.explain.*;

public class CountOperatorExplainer extends CompoundExplainer
{
    public CountOperatorExplainer (String opName, RowType inputType, ValuesRowType resultType, Operator inputOp, ExplainContext context)
    {
        super(Type.COUNT_OPERATOR, buildAtts(opName, inputType, resultType, inputOp, context));
    }
    
    private static Attributes buildAtts (String name, RowType inputType, ValuesRowType resultType, Operator inputOp, ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.INPUT_TYPE, inputType.getExplainer(context));
        atts.put(Label.OUTPUT_TYPE, resultType.getExplainer(context));
        if (inputOp != null)
            atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        
        return atts;
    }
}
