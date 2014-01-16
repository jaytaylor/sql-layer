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

import com.foundationdb.ais.model.ForeignKey;

/**
 * A Join using the Foreign Key 
 * @author TJones-Low
 *
 */
public class TableFKJoin extends BasePlanElement {
    private TableSource parent, child;
    private List<ComparisonCondition> conditions;
    private ForeignKey join;

    public TableFKJoin(TableSource parent, TableSource child,
            List<ComparisonCondition> conditions, ForeignKey join) {
        this.parent = parent;
        this.child = child;
        this.conditions = conditions;
        this.join = join;
    }
    
    public TableSource getParent() {
        return parent;
    }

    public TableSource getChild() {
        return child;
    }

    public List<ComparisonCondition> getConditions() {
        return conditions;
    }

    public ForeignKey getJoin() {
        return join;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            "(" + join + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        parent = (TableSource)parent.duplicate(map);
        child = (TableSource)child.duplicate(map);
        conditions = duplicateList(conditions, map);
    }
    
}
