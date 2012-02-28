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

import com.akiban.ais.model.Group;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A set of tables with common group joins.
 * These joins need not be contiguous, but group join operators can
 * still be used for access among them.
 */
public class TableGroup extends BasePlanElement
{
    private Group group;
    private Set<TableSource> tables;
    private List<TableGroupJoin> joins;

    public TableGroup(Group group) {
        this.group = group;
        tables = new HashSet<TableSource>();
        joins = new ArrayList<TableGroupJoin>();
    }

    public Group getGroup() {
        return group;
    }

    public Set<TableSource> getTables() {
        return tables;
    }

    public List<TableGroupJoin> getJoins() {
        return joins;
    }

    public void addJoin(TableGroupJoin join) {
        joins.add(join);
        tables.add(join.getParent());
        tables.add(join.getChild());
    }

    public void merge(TableGroup other) {
        assert (group == other.group);
        for (TableGroupJoin join : other.joins) {
            join.setGroup(this);
            join.getParent().setGroup(this);
            join.getChild().setGroup(this);
            addJoin(join);
        }
    }

    public int getMinOrdinal() {
        int min = Integer.MAX_VALUE;
        for (TableSource table : tables) {
            int ordinal = table.getTable().getOrdinal();
            if (min > ordinal)
                min = ordinal;
        }
        return min;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            "(" + group.getName() + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tables = duplicateSet(tables, map);
        joins = duplicateList(joins, map);
    }

}
