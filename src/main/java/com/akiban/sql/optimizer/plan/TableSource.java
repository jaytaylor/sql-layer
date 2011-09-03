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

/** A join to an actual table. */
public class TableSource extends BaseJoinable implements ColumnSource
{
    private TableNode table;
    // TODO: Add conditions, correlation name?, ...

    public TableSource(TableNode table) {
        this.table = table;
        table.addUse(this);
    }

    public TableNode getTable() {
        return table;
    }

    @Override
    public String getName() {
        return table.getTable().getName().toString();
    }

    @Override
    public boolean isTable() {
        return true;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }
    
    @Override
    public String toString() {
        return table.toString();
    }
}
