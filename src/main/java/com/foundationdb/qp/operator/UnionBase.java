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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.Strings;


public abstract class UnionBase extends SetOperatorBase {

    UnionBase(Operator left, RowType leftType, Operator right, RowType rightType){
        super( left,  leftType,  right,  rightType);
    }

    @Override
    public String describePlan() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, end = inputs.size(); i < end; ++i) {
            Operator input = inputs.get(i);
            sb.append(input);
            if (i + 1 < end)
                sb.append(Strings.nl()).append("UNION").append(Strings.nl());
        }
        return sb.toString();
    }
}