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

import java.util.HashSet;
import java.util.Set;

/** A contiguous set of tables joined together: flattened / producted
 * and acting as a single row set for higher level joins.
 */
public class TableJoins extends BasePlanWithInput implements Joinable
{
    private TableGroup group;
    private Set<TableSource> tables;
    private PlanNode scan;

    public TableJoins(Joinable joins, TableGroup group) {
        super(joins);
        this.group = group;
        tables = new HashSet<>();
    }

    public TableGroup getGroup() {
        return group;
    }

    public Joinable getJoins() {
        return (Joinable)getInput();
    }

    public Set<TableSource> getTables() {
        return tables;
    }

    public void addTable(TableSource table) {
        assert (group == table.getGroup());
        tables.add(table);
    }

    public PlanNode getScan() {
        return scan;
    }
    public void setScan(PlanNode scan) {
        this.scan = scan;
    }

    @Override
    public boolean isTable() {
        return false;
    }
    @Override
    public boolean isGroup() {
        return true;
    }
    @Override
    public boolean isJoin() {
        return false;
    }
    @Override
    public boolean isInnerJoin() {
        return false;
    }

    @Override
    public String summaryString(PlanToString.Configuration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        str.append(group);
        if (scan != null) {
            str.append(" - ");
            str.append(scan.summaryString(configuration));
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        group = (TableGroup)group.duplicate(map);
        tables = duplicateSet(tables, map);
    }

}
