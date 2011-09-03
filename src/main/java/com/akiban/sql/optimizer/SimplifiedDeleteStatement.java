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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import java.util.*;

/**
 * An SQL DELETE statement turned into a simpler form for the interim
 * heuristic optimizer.
 */
public class SimplifiedDeleteStatement extends SimplifiedTableStatement
{

    public SimplifiedDeleteStatement(DeleteNode delete, Set<ValueNode> joinConditions) {
        super(delete, joinConditions);
    }

    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ntarget: ");
        str.append(getTargetTable());
        if (!getConditions().isEmpty()) {
            str.append("\nconditions: ");
            for (int i = 0; i < getConditions().size(); i++) {
                if (i > 0) str.append(",\n  ");
                str.append(getConditions().get(i));
            }
        }
        return str.toString();
    }

    @Override
    public List<TargetColumn> getTargetColumns() {
        return null;
    }

    @Override
    public ColumnExpressionToIndex getFieldOffset() {
        return null;
    }

}
