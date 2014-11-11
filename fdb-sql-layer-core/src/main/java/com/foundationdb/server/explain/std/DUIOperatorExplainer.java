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
import com.foundationdb.server.explain.*;

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
