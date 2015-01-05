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

import com.foundationdb.ais.model.Group;

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
    private List<TableGroupJoin> joins, rejectedJoins;

    public TableGroup(Group group) {
        this.group = group;
        tables = new HashSet<>();
        joins = new ArrayList<>();
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

    public List<TableGroupJoin> getRejectedJoins() {
        return rejectedJoins;
    }

    public void rejectJoin(TableGroupJoin join) {
        joins.remove(join);
        if (rejectedJoins == null)
            rejectedJoins = new ArrayList<>();
        rejectedJoins.add(join);
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

    public TableSource findByOrdinal(int ordinal) {
        for (TableSource table : tables) {
            if (ordinal == table.getTable().getOrdinal()) {
                return table;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            "(" + group.getName().getTableName() + ")";
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
        if (rejectedJoins != null)
            rejectedJoins = duplicateList(rejectedJoins, map);
    }

}
