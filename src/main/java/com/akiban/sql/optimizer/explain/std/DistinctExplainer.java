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
import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import java.math.BigDecimal;

public class DistinctExplainer extends OperationExplainer
{
    public DistinctExplainer (String name, RowType distinctType, Operator inputOp)
    {
        super(Type.DISTINCT, buildMap(name, distinctType, inputOp));
    }
    
    private static Attributes buildMap (String name, RowType distinctType, Operator inputOp)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.DINSTINCT_TYPE, PrimitiveExplainer.getInstance(distinctType));
        atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer());
        
        return atts;
    }
}
