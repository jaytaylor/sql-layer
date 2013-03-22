
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import java.util.List;
import java.util.Set;

public class FilterExplainer extends CompoundExplainer
{
    public FilterExplainer (String name, Set<RowType> keepType, Operator inputOp, ExplainContext context)
    {
        super(Type.FILTER, buildMap(name, keepType, inputOp, context));
        
    }
    
    private static Attributes buildMap (String name, Set<RowType> keepType, Operator inputOp, ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        
        for (RowType type : keepType)
            atts.put(Label.KEEP_TYPE, type.getExplainer(context));
        return atts;
    }
    
}
