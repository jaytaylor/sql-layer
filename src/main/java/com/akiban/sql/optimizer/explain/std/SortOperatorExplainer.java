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

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;

public class SortOperatorExplainer extends OperationExplainer
{
    public SortOperatorExplainer (String name, API.SortOption sortOption, RowType sortType, Operator inputOp)
    {
        super(Type.SORT, buildMap(name, sortOption, sortType, inputOp));
    }
    
    private static Attributes buildMap (String name, API.SortOption sortOption, RowType sortType, Operator inputOp)
    {
        Attributes map = new Attributes();
        
        map.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        map.put(Label.SORT_OPTION, PrimitiveExplainer.getInstance(sortOption.name()));
        map.put(Label.ROWTYPE, PrimitiveExplainer.getInstance(sortType));
        map.put(Label.INPUT_OPERATOR, inputOp.getExplainer());
        
        return map;
    }
}
