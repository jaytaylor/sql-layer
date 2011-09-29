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

public class GroupScan extends BasePlanNode implements TableLoader
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
        return super.summaryString() + "(" + group.getGroup() + ")";
    }

}
