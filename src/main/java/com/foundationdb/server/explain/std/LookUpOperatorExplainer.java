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
