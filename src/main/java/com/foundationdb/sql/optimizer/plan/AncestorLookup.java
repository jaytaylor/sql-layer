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

package com.foundationdb.sql.optimizer.plan;

import java.util.List;
import java.util.ArrayList;

public class AncestorLookup extends BaseLookup
{
    private TableNode descendant;
    private List<TableNode> ancestors;

    public AncestorLookup(PlanNode input, TableNode descendant,
                          List<TableNode> ancestors,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant;
        this.ancestors = ancestors;
    }

    public AncestorLookup(PlanNode input, TableSource descendant,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant.getTable();
        this.ancestors = new ArrayList<>(tables.size());
        for (TableSource table : getTables()) {
            ancestors.add(table.getTable());
        }
    }

    public TableNode getDescendant() {
        return descendant;
    }

    public List<TableNode> getAncestors() {
        return ancestors;
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + descendant + " -> " + ancestors + ")";
    }

}
