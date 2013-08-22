/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.explain.std;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;

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
