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

package com.akiban.sql.optimizer.plan;

import java.util.List;

public class AncestorLookup extends BasePlanWithInput
{
    private TableSource descendant;
    private List<TableSource> ancestors;

    public AncestorLookup(PlanNode input, 
                          TableSource descendant, List<TableSource> ancestors) {
        super(input);
        this.descendant = descendant;
        this.ancestors = ancestors;
    }

    public TableSource getDescendant() {
        return descendant;
    }

    public List<TableSource> getAncestors() {
        return ancestors;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        descendant = (TableSource)descendant.duplicate();
        ancestors = duplicateList(ancestors, map);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(descendant.getTable());
        str.append(" -> ");
        for (int i = 0; i < ancestors.size(); i++) {
            if (i > 0) str.append(", ");
            str.append(ancestors.get(i).getTable());
        }
        str.append(")");
        return str.toString();
    }

}
