/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.explain.std;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.*;

public class LookUpOperatorExplainer extends OperationExplainer
{
    public LookUpOperatorExplainer (String name,GroupTable gTable, RowType iRowType, boolean keepInput, Operator inputOp)
    {
        super(Type.LOOKUP_OPERATOR, buildAtts(name, gTable, iRowType, keepInput, inputOp));
    }
    
    private static Attributes buildAtts (String name,GroupTable gTable, RowType iRowType, boolean keepInput, Operator inputOp)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        
        // TODO: is anything else needed in Group Table other than  its name?
        atts.put(Label.GROUP_TABLE, PrimitiveExplainer.getInstance(gTable.getName())); 
        
        atts.put(Label.INPUT_TYPE, PrimitiveExplainer.getInstance(iRowType));
        atts.put(Label.LOOK_UP_OPTION, PrimitiveExplainer.getInstance((keepInput ? "" : "DO NOT") + "KEEP INPUT"));
        atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer());
        
        return atts;
    }
}
