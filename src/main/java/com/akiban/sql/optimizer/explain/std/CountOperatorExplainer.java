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

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.sql.optimizer.explain.*;

public class CountOperatorExplainer extends OperationExplainer
{
    public CountOperatorExplainer (String opName, RowType inputType, ValuesRowType resultType, Operator inputOp)
    {
        super(Type.COUNT_OPERATOR, buildAtts(opName, inputType, resultType, inputOp));
    }
    
    private static Attributes buildAtts (String name, RowType inputType, ValuesRowType rstType, Operator inputOp)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.INPUT_TYPE, PrimitiveExplainer.getInstance(inputType));
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(rstType));
        if (inputOp != null) atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer());
        
        return atts;
    }
}
