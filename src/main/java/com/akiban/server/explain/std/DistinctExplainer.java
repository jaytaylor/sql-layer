
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;

public class DistinctExplainer extends CompoundExplainer
{
    public DistinctExplainer (String name, RowType distinctType, Operator inputOp, ExplainContext context)
    {
        super(Type.DISTINCT, buildMap(name, distinctType, inputOp, context));
    }
    
    private static Attributes buildMap (String name, RowType distinctType, Operator inputOp, ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.DINSTINCT_TYPE, distinctType.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        
        return atts;
    }
}
