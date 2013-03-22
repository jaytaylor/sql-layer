
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;

public class SortOperatorExplainer extends CompoundExplainer
{
    public SortOperatorExplainer (String name, API.SortOption sortOption, RowType sortType, Operator inputOp, API.Ordering ordering, ExplainContext context)
    {
        super(Type.SORT, buildMap(name, sortOption, sortType, inputOp, ordering, context));
    }
    
    private static Attributes buildMap (String name, API.SortOption sortOption, RowType sortType, Operator inputOp, API.Ordering ordering, ExplainContext context)
    {
        Attributes map = new Attributes();
        
        map.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        map.put(Label.SORT_OPTION, PrimitiveExplainer.getInstance(sortOption.name()));
        map.put(Label.ROWTYPE, sortType.getExplainer(context));
        map.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        for (int i = 0; i < ordering.sortColumns(); i++)
        {
            if (ordering.usingPVals())
                map.put(Label.EXPRESSIONS, ordering.tExpression(i).getExplainer(context));
            else
                map.put(Label.EXPRESSIONS, ordering.expression(i).getExplainer(context));
            map.put(Label.ORDERING, PrimitiveExplainer.getInstance(ordering.ascending(i) ? "ASC" : "DESC"));
        }
        
        return map;
    }
}
