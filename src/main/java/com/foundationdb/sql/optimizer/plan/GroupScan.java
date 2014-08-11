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

import java.util.Collection;
import java.util.List;

public class GroupScan extends BaseScan implements TableLoader
{
    private TableGroup group;
    private List<TableSource> tables;

    public GroupScan(TableGroup group) {
        this.group = group;
    }

    public TableGroup getGroup() {
        return group;
    }

    /** The tables that this branch lookup introduces into the stream. */
    public List<TableSource> getTables() {
        return tables;
    }

    public void setTables(List<TableSource> tables) {
        this.tables = tables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (tables != null) {
                for (TableSource table : tables) {
                    if (!table.accept(v))
                        break;
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        group = (TableGroup)group.duplicate();
        tables = duplicateList(tables, map);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append('(');
        str.append(group.getGroup());
        if (getCostEstimate() != null) {
            str.append(", ");
            str.append(getCostEstimate());
        }
        str.append(")");
        return str.toString();
    }

    @Override
    public Collection<? extends ConditionExpression> getConditions() {
        return null;
    }
}
